"""First version of the analysis in pandas: correct, simple, unoptimized.

This is the oracle. The Spark version must produce exactly the same outputs,
and the comparison is only meaningful because this version does the plain,
obvious thing first. It runs by default on the deterministic 1% ladder sample
and saves the reference result: the two contract tables plus a summary with
the per-stage row counts that later get validated by logical analysis.

Usage:

    python -m pandas_version.pipeline
    python -m pandas_version.pipeline \\
        --interactions data/raw/amazon_hk/Home_and_Kitchen.0core.rating_only.csv \\
        --output output/analysis/pandas_full

No optimization lives here on purpose: no chunking, no category dtypes, no
column pruning beyond the contract schema. Those belong to the mitigation
experiments, which are measured separately and must not contaminate the
reference version.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import pandas_version.analysis as analysis
from benchmark.config import REPO_ROOT

DEFAULT_INTERACTIONS = (
    REPO_ROOT / "data" / "samples" / "ladder" / "reviews_1pct.parquet"
)
DEFAULT_CATALOG = REPO_ROOT / "data" / "samples" / "ladder" / "catalog.parquet"
DEFAULT_OUTPUT = REPO_ROOT / "output" / "analysis" / "pandas"

KEY_COLUMNS = ["user_id", "parent_asin", "rating", "timestamp"]


def read_interactions(path: Path) -> pd.DataFrame:
    """Read the fact table from Parquet (samples) or CSV (full source)."""
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(
            path,
            usecols=KEY_COLUMNS,
            dtype={
                "user_id": "string",
                "parent_asin": "string",
                "rating": "float64",
                "timestamp": "int64",
            },
        )
    return analysis.read_parquet(path)


def cleaning_report(frame: pd.DataFrame) -> dict:
    """Diagnosis only: how many rows each contract rule would drop.

    Rules are attributed in contract order, so the three counts add up to the
    total dropped. The delivered cleaning is still ``clean_reviews``; this
    function only explains what it does to the data.
    """
    total = len(frame)
    after_nulls = frame.dropna(subset=KEY_COLUMNS)
    after_rating = after_nulls[
        (after_nulls["rating"] >= 1) & (after_nulls["rating"] <= 5)
    ]
    after_timestamp = after_rating[after_rating["timestamp"] > 0]
    return {
        "input_rows": total,
        "dropped_nulls": total - len(after_nulls),
        "dropped_rating": len(after_nulls) - len(after_rating),
        "dropped_timestamp": len(after_rating) - len(after_timestamp),
        "clean_rows": len(after_timestamp),
    }


def run_pipeline(
    interactions: Path, catalog_path: Path, output_dir: Path
) -> dict:
    """Run the analysis and write the two contract tables plus a summary."""
    started = time.perf_counter()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = read_interactions(interactions)
    report = cleaning_report(frame)
    cleaned = analysis.clean_reviews(frame)
    derived = analysis.add_derived_columns(cleaned)
    catalog = analysis.load_catalog(catalog_path)
    joined = analysis.join_with_catalog(derived, catalog)
    store_aggregate = analysis.aggregate_by_store(joined)
    time_aggregate = analysis.aggregate_by_time(joined)
    analysis.write_outputs(store_aggregate, time_aggregate, output_dir)

    summary = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine": "pandas",
        "interactions": str(interactions),
        "catalog": str(catalog_path),
        "output_dir": str(output_dir),
        **report,
        "joined_rows": len(joined),
        "unmatched_rows": int(joined["_meta_matched"].isna().sum()),
        "store_groups": len(store_aggregate),
        "time_groups": len(time_aggregate),
        "seconds": round(time.perf_counter() - started, 3),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    return summary


def print_summary(summary: dict) -> None:
    print("pandas reference run")
    print(f"  interactions      {summary['interactions']}")
    print(f"  input rows        {summary['input_rows']:,}")
    print(f"  dropped nulls     {summary['dropped_nulls']:,}")
    print(f"  dropped rating    {summary['dropped_rating']:,}")
    print(f"  dropped timestamp {summary['dropped_timestamp']:,}")
    print(f"  clean rows        {summary['clean_rows']:,}")
    print(f"  joined rows       {summary['joined_rows']:,}")
    print(f"  without catalog   {summary['unmatched_rows']:,}")
    print(f"  store groups      {summary['store_groups']:,}")
    print(f"  time groups       {summary['time_groups']:,}")
    print(f"  outputs           {summary['output_dir']}")
    print(f"  seconds           {summary['seconds']}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the pandas reference version of the Home_and_Kitchen analysis."
        )
    )
    parser.add_argument("--interactions", default=str(DEFAULT_INTERACTIONS))
    parser.add_argument("--catalog", default=str(DEFAULT_CATALOG))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    summary = run_pipeline(
        Path(args.interactions), Path(args.catalog), Path(args.output)
    )
    print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
