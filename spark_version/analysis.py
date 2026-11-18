"""Spark transformations of the Home_and_Kitchen analysis.

This module is the deliverable, not the measurement harness: the benchmark
imports these functions so what is measured is exactly what is delivered. Each
function is a plain DataFrame transformation with no action and no caching, so
the pandas version can mirror the same rules operation by operation.

Cleaning rules implemented here (they must match the pandas version exactly):

- Drop rows with nulls in: user_id, parent_asin, rating, timestamp.
- Keep only 1 <= rating <= 5 and timestamp > 0.
- Derived columns come from the review timestamp in milliseconds.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

REVIEW_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

META_SCHEMA = StructType(
    [
        StructField("parent_asin", StringType(), True),
        StructField("main_category", StringType(), True),
        StructField("store", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("average_rating", DoubleType(), True),
        StructField("rating_number", LongType(), True),
    ]
)

CATALOG_COLUMNS = [
    "parent_asin",
    "main_category",
    "store",
    "price",
    "average_rating",
    "rating_number",
]


def read_meta_jsonl(spark: SparkSession, meta_path: Path) -> DataFrame:
    """Read the raw metadata JSONL with an explicit schema and dedupe products.

    Used by sample preparation to project the catalog. The analysis itself
    reads the projected Parquet through ``load_catalog``.
    """
    return (
        spark.read.schema(META_SCHEMA)
        .json(str(meta_path))
        .filter(F.col("parent_asin").isNotNull())
        .dropDuplicates(["parent_asin"])
    )


def load_catalog(spark: SparkSession, catalog_path: Path) -> DataFrame:
    """Read the projected product catalog (Parquet) shared by both engines."""
    return (
        spark.read.parquet(str(catalog_path))
        .select(*CATALOG_COLUMNS)
        .withColumn("_meta_matched", F.lit(True))
    )


def clean_reviews(reviews: DataFrame) -> DataFrame:
    """Drop incomplete rows and out-of-range values."""
    return reviews.dropna(
        subset=["user_id", "parent_asin", "rating", "timestamp"]
    ).filter(
        (F.col("rating") >= 1)
        & (F.col("rating") <= 5)
        & (F.col("timestamp") > 0)
    )


def add_derived_columns(reviews: DataFrame) -> DataFrame:
    """Add the date parts the temporal questions need, from the millisecond timestamp."""
    review_timestamp = F.to_timestamp(
        F.col("timestamp").cast("double") / 1000.0
    )
    return (
        reviews.withColumn("review_ts", review_timestamp)
        .withColumn("review_date", F.to_date("review_ts"))
        .withColumn("review_year", F.year("review_ts"))
        .withColumn("review_month", F.month("review_ts"))
        .withColumn("review_dow", F.dayofweek("review_ts"))
        .withColumn("review_hour", F.hour("review_ts"))
    )


def join_with_catalog(reviews: DataFrame, catalog: DataFrame) -> DataFrame:
    """Left join: every review keeps its row, matched products add catalog columns."""
    return reviews.join(catalog, "parent_asin", "left")


def aggregate_by_store(reviews: DataFrame) -> DataFrame:
    """Volume and rating quality per store (the group-by question)."""
    return reviews.groupBy("store").agg(
        F.count("*").alias("reviews"),
        F.countDistinct("parent_asin").alias("products"),
        F.round(F.avg("rating"), 4).alias("avg_rating"),
        F.round(F.stddev("rating"), 4).alias("stddev_rating"),
    )


def aggregate_by_time(reviews: DataFrame) -> DataFrame:
    """Volume and rating quality per year/month/weekday (the temporal question)."""
    return reviews.groupBy("review_year", "review_month", "review_dow").agg(
        F.count("*").alias("reviews"),
        F.round(F.avg("rating"), 4).alias("avg_rating"),
        F.round(F.stddev("rating"), 4).alias("stddev_rating"),
    )


def write_outputs(
    store_aggregate: DataFrame,
    time_aggregate: DataFrame,
    tables_dir: Path,
) -> int:
    """Write both aggregate tables as Parquet and return the part-file count."""
    tables_dir.mkdir(parents=True, exist_ok=True)
    store_aggregate.write.mode("overwrite").parquet(
        str(tables_dir / "agg_by_store.parquet")
    )
    time_aggregate.write.mode("overwrite").parquet(
        str(tables_dir / "agg_by_time.parquet")
    )
    files = [
        path for path in tables_dir.rglob("*.parquet") if path.is_file()
    ]
    return len(files)
