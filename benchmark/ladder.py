"""The comparison ladder: deterministic row fractions and the full catalog.

The ladder answers "how does each engine scale?", so it must hold the analysis
constant and grow the data. Two rules make that honest:

- **Fact rows**: a deterministic hash fraction of the published 0core
  interaction table (1%, 10%, 50%, 100%). ``xxhash64`` over the row's own keys
  makes the selection reproducible on every machine without downloading
  anything extra and without a row order assumption.
- **Catalog**: the full product catalog, prepared once as a projected Parquet
  and read by both engines. Every level joins against the same dimension, so
  the join cost is part of the analysis at every level.

This is a different sample family from the probe's product-cluster sample,
which exists to diagnose the data without downloading the catalog. The ladder
is the one that carries the pandas vs. Spark comparison.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from benchmark.config import (
    RAW_DIR,
    RAW_TEXT_DIR,
    RAW_TEXT_FACTS,
    REVIEW_JSONL_RAW,
    REVIEW_RAW,
    SAMPLE_DIR,
)
from benchmark.sources import download_full, head_file
from spark_version.analysis import (
    CATALOG_COLUMNS,
    REVIEW_SCHEMA,
    read_meta_jsonl,
)
from benchmark.config import META_URL

LADDER_DIR = SAMPLE_DIR / "ladder"
CATALOG_JSONL = RAW_DIR / "meta_Home_and_Kitchen_full.jsonl"
CATALOG_PARQUET = LADDER_DIR / "catalog.parquet"
PROBE_CATALOG_PARQUET = SAMPLE_DIR / "amazon_hk_catalog.parquet"

LEVELS = (0.01, 0.10, 0.50, 1.00)
HASH_MODULUS = 10_000

# Explicit schema for the raw review JSONL. Explicit on purpose: inference
# would read the 31 GB twice. Only the columns the analysis uses are kept.
RAW_REVIEW_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", LongType(), True),
        StructField("text", StringType(), True),
        StructField("title", StringType(), True),
        StructField("helpful_vote", LongType(), True),
        StructField("verified_purchase", BooleanType(), True),
    ]
)


def level_tag(fraction: float) -> str:
    return f"{fraction * 100:g}pct"


def level_sample_path(fraction: float, out_dir: Path | None = None) -> Path:
    out_dir = Path(out_dir) if out_dir else LADDER_DIR
    return out_dir / f"reviews_{level_tag(fraction)}.parquet"


def hash_fraction_condition(fraction: float):
    """Deterministic row filter keeping ~``fraction`` of the interaction rows."""
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    if fraction >= 1:
        return None
    threshold = int(round(fraction * HASH_MODULUS))
    return F.pmod(
        F.xxhash64("user_id", "parent_asin", "rating", "timestamp"),
        F.lit(HASH_MODULUS),
    ) < F.lit(threshold)


def ensure_catalog_parquet(
    spark: SparkSession, meta_jsonl: Path, out_path: Path
) -> Path:
    """Project a metadata JSONL to the catalog Parquet read by both engines."""
    out_path = Path(out_path)
    manifest_path = out_path.with_suffix(".manifest.json")
    source_bytes = Path(meta_jsonl).stat().st_size
    if out_path.exists() and manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            prior.get("source_bytes") == source_bytes
            and prior.get("tool") == "benchmark.measure_sample/catalog-v1"
        ):
            print(f"[catalog] reusing {out_path.name}", flush=True)
            return out_path

    started = time.perf_counter()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    catalog = read_meta_jsonl(spark, meta_jsonl).select(*CATALOG_COLUMNS)
    products = catalog.count()
    if products == 0:
        raise IOError("catalog projection produced zero products")
    catalog.write.mode("overwrite").parquet(str(out_path))
    elapsed = time.perf_counter() - started
    manifest = {
        "source": str(Path(meta_jsonl).name),
        "source_bytes": source_bytes,
        "products": products,
        # Cache key: keep stable or the projection is rebuilt.
        "tool": "benchmark.measure_sample/catalog-v1",
        "seconds": round(elapsed, 3),
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    print(
        f"[catalog] projected {products:,} products to {out_path.name} "
        f"in {elapsed:.1f}s",
        flush=True,
    )
    return out_path


def ensure_full_catalog(spark: SparkSession) -> Path:
    """Download the full metadata once and project it to the shared catalog."""
    LADDER_DIR.mkdir(parents=True, exist_ok=True)
    if not CATALOG_PARQUET.exists():
        size, _etag = head_file(META_URL)
        download_full(META_URL, CATALOG_JSONL, size)
    return ensure_catalog_parquet(spark, CATALOG_JSONL, CATALOG_PARQUET)


def ensure_raw_reviews_parquet(spark: SparkSession) -> Path:
    """Convert the raw review JSONL once to Parquet (explicit schema, 8 columns).

    The raw JSONL is 31.4 GB with text; the converted Parquet is what both
    engines read as facts for the text route. One conversion, reused by every
    ladder level afterwards.
    """
    RAW_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW_TEXT_FACTS.with_suffix(".manifest.json")
    source_bytes = REVIEW_JSONL_RAW.stat().st_size
    if RAW_TEXT_FACTS.exists() and manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            prior.get("source_bytes") == source_bytes
            and prior.get("tool") == "benchmark.measure_sample/raw-text-v1"
        ):
            print(f"[raw] reusing {RAW_TEXT_FACTS.name}", flush=True)
            return RAW_TEXT_FACTS

    started = time.perf_counter()
    frame = spark.read.schema(RAW_REVIEW_SCHEMA).json(str(REVIEW_JSONL_RAW))
    rows = frame.count()
    if rows == 0:
        raise IOError("raw review conversion produced zero rows")
    frame.write.mode("overwrite").parquet(str(RAW_TEXT_FACTS))
    elapsed = time.perf_counter() - started
    facts_bytes = sum(
        path.stat().st_size
        for path in RAW_TEXT_FACTS.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    )
    manifest = {
        "source": REVIEW_JSONL_RAW.name,
        "source_bytes": source_bytes,
        "rows": rows,
        "facts_bytes": facts_bytes,
        # Cache key: keep stable or the conversion is rebuilt.
        "tool": "benchmark.measure_sample/raw-text-v1",
        "seconds": round(elapsed, 3),
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    print(
        f"[raw] converted {rows:,} rows to {RAW_TEXT_FACTS.name} "
        f"({facts_bytes / (1024**3):.2f} GB) in {elapsed:.1f}s",
        flush=True,
    )
    return RAW_TEXT_FACTS


def _read_facts(spark: SparkSession, source: Path):
    """Read the fact table: CSV with header for the 0core route, Parquet for
    the raw-text route."""
    if source.suffix.lower() == ".csv":
        return (
            spark.read.option("header", True)
            .schema(REVIEW_SCHEMA)
            .csv(str(source))
        )
    return spark.read.parquet(str(source))


def ensure_level_sample(
    spark: SparkSession,
    fraction: float,
    source: Path | None = None,
    out_dir: Path | None = None,
    tool: str = "benchmark.measure_sample/ladder-v1",
) -> dict:
    """Build or reuse one deterministic row-fraction sample of the interactions."""
    source = Path(source) if source else REVIEW_RAW
    sample_path = level_sample_path(fraction, out_dir)
    manifest_path = sample_path.with_suffix(".manifest.json")
    review_bytes = source.stat().st_size
    signature = {
        "review_file": source.name,
        "review_bytes": review_bytes,
        "fraction": fraction,
        "hash_modulus": HASH_MODULUS,
        # Cache key: keep stable or every sample is rebuilt.
        "tool": tool,
    }
    if sample_path.exists() and manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if all(prior.get(key) == value for key, value in signature.items()):
            print(f"[ladder] reusing {sample_path.name}", flush=True)
            return prior

    started = time.perf_counter()
    reviews = _read_facts(spark, source)
    total_rows = reviews.count()
    condition = hash_fraction_condition(fraction)
    selected = reviews if condition is None else reviews.filter(condition)
    sample_rows = selected.count()
    if sample_rows == 0:
        raise IOError(f"ladder level {level_tag(fraction)} selected zero rows")
    selected.write.mode("overwrite").parquet(str(sample_path))
    elapsed = time.perf_counter() - started
    sample_bytes = sum(
        path.stat().st_size
        for path in sample_path.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    )
    manifest = {
        **signature,
        "total_rows": total_rows,
        "sample_rows": sample_rows,
        "actual_fraction": round(sample_rows / total_rows, 6),
        "sample_path": str(sample_path),
        "sample_bytes": sample_bytes,
        "seconds": round(elapsed, 3),
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    print(
        f"[ladder] built {sample_path.name}: {sample_rows:,} of "
        f"{total_rows:,} rows ({manifest['actual_fraction'] * 100:.4f}%) "
        f"in {elapsed:.1f}s",
        flush=True,
    )
    return manifest
