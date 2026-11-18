"""Command line entry point of the optimized Spark version.

Example (Windows):

    .venv\\Scripts\\python -m spark_version.run ^
        --reviews data/samples/ladder/<sample>.parquet ^
        --catalog data/samples/ladder/catalog.parquet ^
        --output output/spark_optimized/pct_010 ^
        --explain output/spark_optimized/pct_010/plan.txt

Each optimization can be switched off to measure its effect on its own
(--no-broadcast, --cache, --shuffle-partitions).
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from spark_version.optimized import (
    OptimizationOptions,
    default_shuffle_partitions,
    run_optimized,
)
from spark_version.session import build_spark_session


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the command line options."""
    parser = argparse.ArgumentParser(description="Optimized Spark version of the analysis.")
    parser.add_argument("--reviews", required=True, type=Path, help="Review sample (Parquet)")
    parser.add_argument("--catalog", required=True, type=Path, help="Projected catalog (Parquet)")
    parser.add_argument("--output", required=True, type=Path, help="Directory for the two tables")
    parser.add_argument("--explain", type=Path, default=None, help="Save the physical plans here")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--no-broadcast", action="store_true", help="Disable the broadcast join")
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Cache the cleaned reviews (off by default: slower in our measurements)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=default_shuffle_partitions(),
        help="spark.sql.shuffle.partitions (default: 2 per CPU core)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the optimized pipeline and print a one-line JSON summary."""
    arguments = parse_arguments(argv)
    options = OptimizationOptions(
        broadcast_catalog=not arguments.no_broadcast,
        cache_base=arguments.cache,
        shuffle_partitions=arguments.shuffle_partitions,
    )
    spark = build_spark_session(app_name="spark-optimized", driver_memory=arguments.driver_memory)
    started = time.perf_counter()
    try:
        counts = run_optimized(
            spark,
            arguments.reviews,
            arguments.catalog,
            arguments.output,
            options,
            explain_path=arguments.explain,
        )
    finally:
        spark.stop()
    summary = {
        **counts,
        "seconds": round(time.perf_counter() - started, 2),
        "broadcast": options.broadcast_catalog,
        "cache": options.cache_base,
        "shuffle_partitions": options.shuffle_partitions,
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
