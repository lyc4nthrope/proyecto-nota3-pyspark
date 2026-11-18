"""Optimized Spark pipeline for the Home_and_Kitchen analysis.

It reuses the transformations of ``spark_version.analysis`` (same cleaning,
derived columns, join and aggregates, so the outputs stay equivalent) and only
changes *how* Spark executes them. Each optimization targets a cost visible in
the baseline physical plan:

1. Column pruning: read only the four review columns the analysis uses.
   Parquet is columnar, so the other columns are never read from disk.
2. No join for the temporal table: it does not use any catalog column, so it
   is computed from the reviews alone. That removes a whole join and shuffle.
3. Broadcast join for the store table: the catalog is reduced to
   ``parent_asin`` + ``store`` and copied to every executor, so the 66 M review
   rows are joined where they are instead of being shuffled by key.
4. Cache (optional, OFF by default): the cleaned reviews feed two
   aggregations, and because Spark evaluates lazily each write re-reads and
   re-cleans the input. Measured on 20 M synthetic rows, caching was SLOWER
   (28.8 s vs 25.7 s): re-reading four pruned Parquet columns costs less than
   storing 20 M rows. It stays available (--cache) to be measured on real data.
5. Shuffle partitions sized to the machine: the default of 200 creates many
   tiny tasks in local mode; a few tasks per core is enough.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_version import analysis

REVIEW_COLUMNS = ["user_id", "parent_asin", "rating", "timestamp"]
# Columns the two aggregations need after cleaning; everything else is dropped
# before caching so the cached data is as small as possible. ``text_length``
# joins them because the contract's aggregates report ``avg_text_length``
# (adapted to the text route of T1).
BASE_COLUMNS = [
    "parent_asin",
    "rating",
    "review_year",
    "review_month",
    "review_dow",
    "text_length",
]
CATALOG_JOIN_COLUMNS = ["parent_asin", "store"]


@dataclass(frozen=True)
class OptimizationOptions:
    """Switches for each optimization, so each one can be measured on its own.

    Attributes:
        broadcast_catalog: Use a broadcast hash join for the catalog.
        cache_base: Keep the cleaned reviews in memory (spilling to disk if needed).
        shuffle_partitions: Value for ``spark.sql.shuffle.partitions``;
            None keeps Spark's current setting.
    """

    broadcast_catalog: bool = True
    cache_base: bool = False
    shuffle_partitions: int | None = None


def default_shuffle_partitions() -> int:
    """Return a few shuffle tasks per CPU core (local mode)."""
    return max(4, (os.cpu_count() or 4) * 2)


def read_reviews(spark: SparkSession, reviews_path: Path) -> DataFrame:
    """Read only the review columns the analysis uses (column pruning)."""
    return spark.read.parquet(str(reviews_path)).select(*REVIEW_COLUMNS)


def read_catalog_for_join(spark: SparkSession, catalog_path: Path) -> DataFrame:
    """Read only the catalog columns the store table needs."""
    return spark.read.parquet(str(catalog_path)).select(*CATALOG_JOIN_COLUMNS)


def build_base(reviews: DataFrame, options: OptimizationOptions) -> DataFrame:
    """Clean the reviews, add the date parts and keep only the needed columns."""
    base = analysis.add_derived_columns(analysis.clean_reviews(reviews)).select(*BASE_COLUMNS)
    if options.cache_base:
        base = base.persist(StorageLevel.MEMORY_AND_DISK)
    return base


def store_aggregate(
    base: DataFrame, catalog: DataFrame, options: OptimizationOptions
) -> DataFrame:
    """Store table: left join with the catalog, then aggregate by store."""
    right_side = F.broadcast(catalog) if options.broadcast_catalog else catalog
    return analysis.aggregate_by_store(analysis.join_with_catalog(base, right_side))


def time_aggregate(base: DataFrame) -> DataFrame:
    """Temporal table: computed without the join, since it uses no catalog column."""
    return analysis.aggregate_by_time(base)


def run_optimized(
    spark: SparkSession,
    reviews_path: Path,
    catalog_path: Path,
    tables_dir: Path,
    options: OptimizationOptions = OptimizationOptions(),
    explain_path: Path | None = None,
) -> dict:
    """Run the full optimized pipeline and write both tables as Parquet.

    Args:
        spark: Active session.
        reviews_path: Review sample in Parquet (file or Spark part directory).
        catalog_path: Projected catalog in Parquet.
        tables_dir: Output directory; receives agg_by_store.parquet and
            agg_by_time.parquet, the same names as the baseline.
        options: Which optimizations are on.
        explain_path: If given, the formatted physical plans are saved there
            (evidence of the DAG for the report).

    Returns:
        Row counts: cleaned reviews and rows of each output table.
    """
    if options.shuffle_partitions is not None:
        spark.conf.set("spark.sql.shuffle.partitions", str(options.shuffle_partitions))

    base = build_base(read_reviews(spark, reviews_path), options)
    by_store = store_aggregate(base, read_catalog_for_join(spark, catalog_path), options)
    by_time = time_aggregate(base)

    if explain_path is not None:
        explain_path.parent.mkdir(parents=True, exist_ok=True)
        explain_path.write_text(
            "== agg_by_store ==\n"
            + physical_plan(by_store)
            + "\n== agg_by_time ==\n"
            + physical_plan(by_time),
            encoding="utf-8",
        )

    try:
        # The first write fills the cache; the second write reuses it. No extra
        # count() here: on purpose, since it would be one more full pass.
        # The aggregates are small: one file each instead of one per shuffle task.
        analysis.write_outputs(by_store.coalesce(1), by_time.coalesce(1), tables_dir)
    finally:
        if options.cache_base:
            base.unpersist()

    # Counts come from the small written tables, not from the big input.
    written_time = spark.read.parquet(str(tables_dir / "agg_by_time.parquet"))
    return {
        "cleaned_rows": int(written_time.agg(F.sum("reviews")).first()[0] or 0),
        "store_rows": spark.read.parquet(str(tables_dir / "agg_by_store.parquet")).count(),
        "time_rows": written_time.count(),
    }


def physical_plan(frame: DataFrame) -> str:
    """Return the formatted physical plan Spark will execute for a DataFrame."""
    return frame._jdf.queryExecution().explainString(
        frame.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
    )
