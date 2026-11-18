"""Orchestration of a probe run: sample assembly, measured operations, report data.

The measured operations are the deliverable transformations from
``spark_version.analysis``; this module only forces them with an action so the
instrument can record time and memory. Nothing here optimizes: no cache, no
broadcast hint, no repartition and no tuned shuffle.
"""

from __future__ import annotations

import json
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil
import pyspark
from pyspark.sql import SparkSession, functions as F

from benchmark.config import (
    DATASET_ID,
    REPO_ROOT,
    SAMPLE_DIR,
    SAMPLE_PATH,
    tables_dir_for,
)
from benchmark.instrument import OperationRecorder
from spark_version.analysis import (
    REVIEW_SCHEMA,
    add_derived_columns,
    aggregate_by_store,
    aggregate_by_time,
    clean_reviews,
    join_with_catalog,
    load_catalog,
    read_meta_jsonl,
    write_outputs,
)


def ensure_sample(
    spark: SparkSession,
    sources: dict,
    rebuild: bool = False,
) -> tuple[dict, bool]:
    """Build or reuse the product-cluster review sample."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = SAMPLE_PATH.with_suffix(".manifest.json")
    signature = {
        "review_url": sources["review_url"],
        "review_bytes": sources["review_bytes"],
        "review_etag": sources["review_etag"],
        "meta_url": sources["meta_url"],
        "meta_bytes": sources["meta_bytes"],
        "meta_etag": sources["meta_etag"],
        "meta_fraction": sources["meta_manifest"]["fraction"],
        "meta_windows": sources["meta_manifest"]["windows"],
        # Cache key: keep stable or every stored sample is rebuilt.
        "tool": "benchmark.measure_sample/review-cluster-v1",
    }
    if not rebuild and SAMPLE_PATH.exists() and manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if all(prior.get(key) == value for key, value in signature.items()):
            print(f"[sample] reusing {SAMPLE_PATH.name}", flush=True)
            return prior, False

    started = time.perf_counter()
    meta = read_meta_jsonl(spark, sources["meta_path"])
    meta_products = meta.count()
    reviews = (
        spark.read.option("header", True)
        .schema(REVIEW_SCHEMA)
        .csv(str(sources["review_path"]))
    )
    review_rows = reviews.count()
    sample = reviews.join(meta.select("parent_asin"), "parent_asin", "inner")
    sample_rows = sample.count()
    if sample_rows == 0:
        raise IOError("the product-cluster sample contains zero reviews")
    sample.coalesce(1).write.mode("overwrite").parquet(str(SAMPLE_PATH))
    elapsed = time.perf_counter() - started
    actual_fraction = sample_rows / review_rows if review_rows else 0.0
    manifest = {
        **signature,
        "review_rows": review_rows,
        "sample_rows": sample_rows,
        "actual_fraction": round(actual_fraction, 6),
        "meta_products": meta_products,
        "sample_path": str(SAMPLE_PATH.relative_to(REPO_ROOT)),
        "build_seconds": round(elapsed, 3),
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pyspark_version": pyspark.__version__,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    print(
        f"[sample] built {SAMPLE_PATH.name}: {sample_rows:,} of "
        f"{review_rows:,} reviews ({actual_fraction * 100:.4f}%) from "
        f"{meta_products:,} products",
        flush=True,
    )
    return manifest, True


def run_analysis(
    spark: SparkSession,
    sample_path: Path,
    catalog_path: Path,
    sample_rows: int,
) -> list[dict]:
    """Execute the baseline analysis operations one by one, recording each."""
    recorder = OperationRecorder()
    context: dict = {"sample_rows": sample_rows}

    def read_operation() -> str:
        frame = spark.read.parquet(str(sample_path))
        row_count = frame.count()
        context["frame"] = frame
        return f"{row_count:,} rows"

    def clean_operation() -> str:
        cleaned = clean_reviews(context["frame"])
        row_count = cleaned.count()
        context["cleaned"] = cleaned
        dropped = context["sample_rows"] - row_count
        return f"{row_count:,} kept; {dropped:,} dropped"

    def derive_operation() -> str:
        derived = add_derived_columns(context["cleaned"])
        row_count = derived.count()
        context["derived"] = derived
        return f"{row_count:,} rows; 6 derived columns"

    def join_operation() -> str:
        catalog = load_catalog(spark, catalog_path)
        joined = join_with_catalog(context["derived"], catalog)
        stats = joined.agg(
            F.count("*").alias("rows"),
            F.sum(
                F.when(F.col("_meta_matched").isNull(), 1).otherwise(0)
            ).alias("unmatched"),
        ).first()
        context["joined"] = joined
        return (
            f"{int(stats['rows']):,} rows; "
            f"{int(stats['unmatched']):,} without metadata"
        )

    def store_aggregate_operation() -> str:
        aggregated = aggregate_by_store(context["joined"])
        groups = aggregated.count()
        top = (
            aggregated.orderBy(F.desc("reviews")).limit(1).collect()
        )
        context["store_aggregate"] = aggregated
        if top:
            name = top[0]["store"] or "(no store)"
            return (
                f"{groups:,} stores; top {name} "
                f"({top[0]['reviews']:,} reviews, {top[0]['products']:,} products)"
            )
        return f"{groups:,} stores"

    def time_aggregate_operation() -> str:
        aggregated = aggregate_by_time(context["joined"])
        groups = aggregated.count()
        top = (
            aggregated.orderBy(F.desc("reviews")).limit(1).collect()
        )
        context["time_aggregate"] = aggregated
        if top:
            return (
                f"{groups:,} groups; top "
                f"{int(top[0]['review_year'])}-{int(top[0]['review_month']):02d} "
                f"({top[0]['reviews']:,} reviews)"
            )
        return f"{groups:,} groups"

    def write_operation() -> str:
        parts = write_outputs(
            context["store_aggregate"],
            context["time_aggregate"],
            tables_dir_for("spark"),
        )
        return f"2 tables; {parts} Parquet part files"

    recorder.run("read", (), read_operation)
    recorder.run("clean", ("read",), clean_operation)
    recorder.run("derive", ("clean",), derive_operation)
    recorder.run("join", ("derive",), join_operation)
    recorder.run("aggregate_by_store", ("join",), store_aggregate_operation)
    recorder.run("aggregate_by_time", ("join",), time_aggregate_operation)
    recorder.run(
        "write_parquet",
        ("aggregate_by_store", "aggregate_by_time"),
        write_operation,
    )
    return recorder.results


def environment_info(spark: SparkSession) -> dict:
    memory = psutil.virtual_memory()
    return {
        "dataset": DATASET_ID,
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "java": spark.sparkContext._jvm.System.getProperty("java.version"),
        "os": platform.platform(),
        "cpu_logical": psutil.cpu_count(logical=True),
        "ram_total_gb": round(memory.total / (1024**3), 1),
        "spark_master": spark.sparkContext.master,
        "spark_driver_memory": spark.conf.get("spark.driver.memory", "unset"),
    }
