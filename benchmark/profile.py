"""One-pass diagnostic profile of the assembled sample.

Profiling is diagnosis, not cleaning: it reports what is in the data, how much
of it is wrong and what it weighs, so the cleaning rules can be written from
measured facts instead of assumptions.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _timestamp_text(value) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def profile_sample(spark: SparkSession, sample_path: Path) -> dict:
    """One-pass profile of the review sample: rows, nulls, ratings, coverage."""
    sample = spark.read.parquet(str(sample_path))
    fields = sample.schema.fields
    expressions = [F.count("*").alias("rows")]
    for field in fields:
        name = field.name
        expressions.append(
            F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(
                f"null__{name}"
            )
        )
    expressions.extend(
        [
            F.min("rating").alias("rating_min"),
            F.max("rating").alias("rating_max"),
            F.avg("rating").alias("rating_avg"),
            F.min("timestamp").alias("timestamp_min"),
            F.max("timestamp").alias("timestamp_max"),
            F.countDistinct("user_id").alias("distinct_users"),
            F.countDistinct("parent_asin").alias("distinct_products"),
        ]
    )
    for rating in range(1, 6):
        expressions.append(
            F.sum(
                F.when(F.col("rating") == float(rating), 1).otherwise(0)
            ).alias(f"rating_{rating}")
        )
    row = sample.agg(*expressions).first().asDict(recursive=True)
    rows = int(row["rows"])
    columns = []
    for field in fields:
        name = field.name
        nulls = int(row.get(f"null__{name}") or 0)
        entry = {
            "name": name,
            "type": field.dataType.simpleString(),
            "nulls": nulls,
            "null_pct": round(nulls / rows * 100, 2) if rows else 0.0,
            "min": "",
            "max": "",
            "mean": "",
        }
        if name == "rating":
            entry["min"] = f"{float(row['rating_min']):.2f}"
            entry["max"] = f"{float(row['rating_max']):.2f}"
            entry["mean"] = f"{float(row['rating_avg']):.4f}"
        elif name == "timestamp":
            entry["min"] = _timestamp_text(row["timestamp_min"])
            entry["max"] = _timestamp_text(row["timestamp_max"])
        columns.append(entry)
    distinct_products = int(row["distinct_products"])
    disk_bytes = sum(
        path.stat().st_size
        for path in sample_path.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    )
    return {
        "rows": rows,
        "columns": len(fields),
        "disk_mb": round(disk_bytes / (1024**2), 2),
        "distinct_users": int(row["distinct_users"]),
        "distinct_products": distinct_products,
        "reviews_per_product": round(rows / distinct_products, 2)
        if distinct_products
        else 0.0,
        "timestamp_min": _timestamp_text(row["timestamp_min"]),
        "timestamp_max": _timestamp_text(row["timestamp_max"]),
        "rating_counts": [
            {"rating": rating, "reviews": int(row[f"rating_{rating}"] or 0)}
            for rating in range(1, 6)
        ],
        "column_profile": columns,
    }
