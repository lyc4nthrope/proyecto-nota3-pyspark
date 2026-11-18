"""Entry point of the baseline measurement probe for Amazon Home_and_Kitchen.

This module is the command line only. The work lives in small modules by
responsibility:

- ``benchmark.config``   -- which data is measured and where results go.
- ``benchmark.sources``  -- deterministic download and byte-window sampling.
- ``benchmark.instrument`` -- the instrument: time and peak process-tree RSS.
- ``benchmark.profile``  -- diagnosis of the assembled sample.
- ``benchmark.runner``   -- sample assembly and the measured operations.
- ``benchmark.report``   -- Markdown/CSV/JSON rendering.
- ``spark_version.analysis`` -- the delivered Spark transformations.

This is NOT the final pipeline and NOT an optimization: there is no cache, no
broadcast hint, no repartition, and no tuned shuffle. Each analysis operation is
forced with an action so its wall time and peak process-tree RSS can be
recorded. Spark is lazy and nothing is cached, so operation N re-executes
operations 1..N; the report labels the times as cumulative.

Usage:
    .venv\\Scripts\\python -m benchmark.measure_sample
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from benchmark.config import (
    DATASET_ID,
    DEFAULT_FRACTION,
    DEFAULT_META_WINDOWS,
    OUTPUT_DIR,
    REPO_ROOT,
    TABLES_DIR,
)
from benchmark.ladder import PROBE_CATALOG_PARQUET, ensure_catalog_parquet
from benchmark.instrument import measure_operation
from benchmark.profile import profile_sample
from benchmark.report import render_report, save_report
from benchmark.runner import ensure_sample, environment_info, run_analysis
from benchmark.sources import ensure_sources
from spark_version.session import build_spark_session


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure baseline operations on a deterministic 1% Home_and_Kitchen "
            "product-cluster sample."
        )
    )
    parser.add_argument("--fraction", type=float, default=DEFAULT_FRACTION)
    parser.add_argument("--meta-windows", type=int, default=DEFAULT_META_WINDOWS)
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--rebuild-sample", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = build_spark_session(
        app_name="amazon-home-kitchen-1pct-measurement",
        driver_memory=args.driver_memory,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        environment = environment_info(spark)
        preparation: list[dict] = []
        source_state: dict = {}
        sample_state: dict = {}
        profile_state: dict = {}

        def download_action() -> str:
            source_state.update(
                ensure_sources(
                    args.fraction, args.meta_windows, force=args.force_download
                )
            )
            meta_manifest = source_state["meta_manifest"]
            return (
                f"review {source_state['review_bytes'] / (1024**2):.1f} MB + "
                f"metadata {meta_manifest['fetched_bytes'] / (1024**2):.1f} MB "
                f"fetched; {meta_manifest['products']:,} products sampled"
            )

        download_step = measure_operation("download_source", download_action)
        preparation.append(
            {
                **{
                    key: download_step[key]
                    for key in download_step
                    if key != "value"
                },
                "result": download_step["value"],
            }
        )
        if download_step["error"]:
            print(f"[fatal] source download failed: {download_step['error']}")
            return 1

        def sample_action() -> str:
            manifest, built = ensure_sample(
                spark, source_state, rebuild=args.rebuild_sample
            )
            sample_state.update(manifest)
            suffix = "" if built else " (reused)"
            return (
                f"{manifest['sample_rows']:,} rows; "
                f"{manifest['actual_fraction'] * 100:.4f}% of "
                f"{manifest['review_rows']:,}{suffix}"
            )

        sample_step = measure_operation("build_sample", sample_action)
        preparation.append(
            {
                **{
                    key: sample_step[key]
                    for key in sample_step
                    if key != "value"
                },
                "result": sample_step["value"],
            }
        )
        if sample_step["error"]:
            print(f"[fatal] sample build failed: {sample_step['error']}")
            return 1

        catalog_state: dict = {}

        def catalog_action() -> str:
            catalog_path = ensure_catalog_parquet(
                spark, Path(source_state["meta_path"]), PROBE_CATALOG_PARQUET
            )
            catalog_state["catalog_path"] = catalog_path
            manifest = json.loads(
                catalog_path.with_suffix(".manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            return f"{manifest['products']:,} products -> {catalog_path.name}"

        catalog_step = measure_operation("prepare_catalog", catalog_action)
        preparation.append(
            {
                **{
                    key: catalog_step[key]
                    for key in catalog_step
                    if key != "value"
                },
                "result": catalog_step["value"],
            }
        )
        if catalog_step["error"]:
            print(f"[fatal] catalog preparation failed: {catalog_step['error']}")
            return 1

        def profile_action() -> str:
            profile_state.update(
                profile_sample(spark, REPO_ROOT / sample_state["sample_path"])
            )
            return (
                f"{profile_state['rows']:,} rows; "
                f"{profile_state['columns']} columns; "
                f"{profile_state['disk_mb']:.2f} MB"
            )

        profile_step = measure_operation("profile_sample", profile_action)
        preparation.append(
            {
                **{
                    key: profile_step[key]
                    for key in profile_step
                    if key != "value"
                },
                "result": profile_step["value"],
            }
        )
        if profile_step["error"]:
            print(f"[warn] profile failed: {profile_step['error']}")
            profile_state = {
                "rows": sample_state["sample_rows"],
                "columns": 4,
                "disk_mb": 0.0,
                "distinct_users": 0,
                "distinct_products": sample_state.get("meta_products", 0),
                "reviews_per_product": 0.0,
                "timestamp_min": "",
                "timestamp_max": "",
                "rating_counts": [],
                "column_profile": [],
            }

        if TABLES_DIR.exists():
            shutil.rmtree(TABLES_DIR)
        TABLES_DIR.mkdir(parents=True, exist_ok=True)
        operations = run_analysis(
            spark,
            REPO_ROOT / sample_state["sample_path"],
            Path(catalog_state["catalog_path"]),
            sample_state["sample_rows"],
        )
        report = {
            "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "argv": [Path(sys.executable).name, *sys.argv],
            "environment": environment,
            "source": {
                "dataset": DATASET_ID,
                "review_table": "benchmark/0core/rating_only/Home_and_Kitchen.csv",
                "review_mb": source_state["review_bytes"] / (1024**2),
                "meta_table": "raw/meta_categories/meta_Home_and_Kitchen.jsonl",
                "meta_mb": source_state["meta_bytes"] / (1024**2),
                "meta_sample_mb": source_state["meta_manifest"]["sample_bytes"]
                / (1024**2),
                "meta_windows": source_state["meta_manifest"]["windows"],
            },
            "sample": {
                "review_rows": sample_state["review_rows"],
                "sample_rows": sample_state["sample_rows"],
                "actual_fraction": sample_state["actual_fraction"],
                "meta_products": sample_state["meta_products"],
                "sample_path": sample_state["sample_path"],
            },
            "profile": profile_state,
            "preparation": preparation,
            "operations": operations,
        }
        save_report(report)
        print()
        print(render_report(report))
        print()
        print(f"Saved: {OUTPUT_DIR / 'report.md'}")
        print(f"Saved: {OUTPUT_DIR / 'measurements.csv'}")
        print(f"Saved: {OUTPUT_DIR / 'run.json'}")
        return 1 if any(step["error"] for step in operations) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
