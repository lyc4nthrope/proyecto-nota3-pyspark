"""Measured pandas operations, mirroring ``benchmark.runner.run_analysis``.

Same operation names, same order and same result strings as the Spark runner,
so a row-by-row comparison of the two engines is possible. The extra
``frame_mb`` field reports the deep in-memory size of the main DataFrame: that
is the number that decides whether pandas fits in the test machine's RAM.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import pandas_version.analysis as analysis
from benchmark.config import tables_dir_for
from benchmark.instrument import OperationRecorder


def pandas_environment_info() -> dict:
    import platform

    import psutil

    memory = psutil.virtual_memory()
    return {
        "python": platform.python_version(),
        "pandas": pd.__version__,
        "engine": "pandas",
        "os": platform.platform(),
        "cpu_logical": psutil.cpu_count(logical=True),
        "ram_total_gb": round(memory.total / (1024**3), 1),
    }


def _frame_mb(frame: pd.DataFrame) -> float:
    return round(frame.memory_usage(deep=True).sum() / (1024**2), 1)


def run_pandas_analysis(
    sample_path: Path, catalog_path: Path, sample_rows: int
) -> list[dict]:
    """Execute the baseline analysis operations one by one, recording each."""
    recorder = OperationRecorder()
    context: dict = {"sample_rows": sample_rows}

    def read_operation() -> dict:
        frame = analysis.read_parquet(sample_path)
        context["frame"] = frame
        return {
            "result": f"{len(frame):,} rows",
            "frame_mb": _frame_mb(frame),
        }

    def clean_operation() -> dict:
        cleaned = analysis.clean_reviews(context["frame"])
        context["cleaned"] = cleaned
        dropped = context["sample_rows"] - len(cleaned)
        return {
            "result": f"{len(cleaned):,} kept; {dropped:,} dropped",
            "frame_mb": _frame_mb(cleaned),
        }

    def derive_operation() -> dict:
        derived = analysis.add_derived_columns(context["cleaned"])
        context["derived"] = derived
        return {
            "result": f"{len(derived):,} rows; 6 derived columns",
            "frame_mb": _frame_mb(derived),
        }

    def join_operation() -> dict:
        catalog = analysis.load_catalog(catalog_path)
        joined = analysis.join_with_catalog(context["derived"], catalog)
        context["joined"] = joined
        unmatched = int(joined["_meta_matched"].isna().sum())
        return {
            "result": f"{len(joined):,} rows; {unmatched:,} without metadata",
            "frame_mb": _frame_mb(joined),
        }

    def store_aggregate_operation() -> dict:
        aggregated = analysis.aggregate_by_store(context["joined"])
        context["store_aggregate"] = aggregated
        if len(aggregated):
            top = aggregated.sort_values(
                "reviews", ascending=False
            ).iloc[0]
            name = top["store"]
            if name is None or (isinstance(name, float) and pd.isna(name)) or name == "":
                name = "(no store)"
            return {
                "result": (
                    f"{len(aggregated):,} stores; top {name} "
                    f"({int(top['reviews']):,} reviews, "
                    f"{int(top['products']):,} products)"
                ),
                "frame_mb": _frame_mb(aggregated),
            }
        return {
            "result": f"{len(aggregated):,} stores",
            "frame_mb": _frame_mb(aggregated),
        }

    def time_aggregate_operation() -> dict:
        aggregated = analysis.aggregate_by_time(context["joined"])
        context["time_aggregate"] = aggregated
        if len(aggregated):
            top = aggregated.sort_values("reviews", ascending=False).iloc[0]
            return {
                "result": (
                    f"{len(aggregated):,} groups; top "
                    f"{int(top['review_year'])}-{int(top['review_month']):02d} "
                    f"({int(top['reviews']):,} reviews)"
                ),
                "frame_mb": _frame_mb(aggregated),
            }
        return {
            "result": f"{len(aggregated):,} groups",
            "frame_mb": _frame_mb(aggregated),
        }

    def write_operation() -> dict:
        parts = analysis.write_outputs(
            context["store_aggregate"],
            context["time_aggregate"],
            tables_dir_for("pandas"),
        )
        return {"result": f"2 tables; {parts} Parquet part files"}

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
