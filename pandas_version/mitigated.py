"""Pandas mitigation attempts required before declaring it unviable.

The assignment demands trying pandas' own techniques first: column selection,
optimized dtypes and chunked reading. Each function runs the SAME analysis as
``pandas_version.pipeline`` with one mitigation applied, so the outputs stay
comparable and the only difference under test is the technique.

Run one technique on one input:

    python -m pandas_version.mitigated --technique columns \\
        --interactions data/samples/ladder_raw/reviews_100pct.parquet \\
        --catalog data/samples/ladder/catalog.parquet --output output/mitigations/columns_100pct

    python -m pandas_version.mitigated --technique chunks \\
        --interactions data/raw/amazon_hk/reviews_Home_and_Kitchen_raw.jsonl \\
        --catalog data/samples/ladder/catalog.parquet --max-rows 673379 \\
        --output output/mitigations/chunks_1pct

Every run writes the two contract tables plus a ``summary.json`` with the
numbers the mitigation table needs.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import pandas_version.analysis as analysis

# Columns the contract's analysis uses. The raw source has more (title,
# images, helpful_vote, verified_purchase): everything else is waste.
ANALYSIS_COLUMNS = ["user_id", "parent_asin", "rating", "timestamp", "text"]
DATE_PARTS = ["review_year", "review_month", "review_dow", "review_hour"]


def _frame_mb(frame: pd.DataFrame) -> float:
    return round(frame.memory_usage(deep=True).sum() / (1024**2), 1)


def _summary(technique: str, frames_mb: dict, rows: int, seconds: float, out_dir: Path, notes: str = "") -> dict:
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "technique": technique,
        "rows": rows,
        "seconds": round(seconds, 3),
        "frames_mb": frames_mb,
        "output_dir": str(out_dir),
        "notes": notes,
    }


def _write(tables_dir: Path, store_aggregate, time_aggregate, summary: dict) -> None:
    analysis.write_outputs(store_aggregate, time_aggregate, tables_dir)
    (tables_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8"
    )


def run_column_selection(interactions: Path, catalog_path: Path, out_dir: Path) -> dict:
    """Mitigation 1: read only the columns the analysis uses.

    The raw source carries title, images and votes that no question needs.
    ``text`` stays because the contract reports ``avg_text_length``.
    """
    started = time.perf_counter()
    frame = pd.read_parquet(interactions, columns=ANALYSIS_COLUMNS)
    frames = {"read": _frame_mb(frame)}
    cleaned = analysis.clean_reviews(frame)
    derived = analysis.add_derived_columns(cleaned)
    frames["derived"] = _frame_mb(derived)
    joined = analysis.join_with_catalog(derived, analysis.load_catalog(catalog_path))
    frames["joined"] = _frame_mb(joined)
    store_aggregate = analysis.aggregate_by_store(joined)
    time_aggregate = analysis.aggregate_by_time(joined)
    summary = _summary(
        "columns", frames, len(frame), time.perf_counter() - started, out_dir,
        notes="text is a contract column, so it cannot be dropped",
    )
    _write(out_dir, store_aggregate, time_aggregate, summary)
    return summary


def run_optimized_dtypes(interactions: Path, catalog_path: Path, out_dir: Path) -> dict:
    """Mitigation 2: compact dtypes where they actually pay off.

    ``rating`` to float32, the date parts to the smallest int that fits, and
    the catalog strings to pandas ``category``. The review text cannot be
    narrowed: its bytes are the data.
    """
    started = time.perf_counter()
    frame = pd.read_parquet(interactions, columns=ANALYSIS_COLUMNS)
    frame["rating"] = frame["rating"].astype("float32")
    frames = {"read": _frame_mb(frame)}
    cleaned = analysis.clean_reviews(frame)
    derived = analysis.add_derived_columns(cleaned)
    derived["rating"] = derived["rating"].astype("float32")
    derived["review_year"] = derived["review_year"].astype("int16")
    for name in ("review_month", "review_dow", "review_hour"):
        derived[name] = derived[name].astype("int8")
    frames["derived"] = _frame_mb(derived)
    catalog = analysis.load_catalog(catalog_path)
    for name in ("store", "main_category"):
        catalog[name] = catalog[name].astype("category")
    joined = analysis.join_with_catalog(derived, catalog)
    frames["joined"] = _frame_mb(joined)
    store_aggregate = analysis.aggregate_by_store(joined)
    time_aggregate = analysis.aggregate_by_time(joined)
    summary = _summary(
        "dtypes", frames, len(frame), time.perf_counter() - started, out_dir,
        notes="text cannot be narrowed: its bytes are the data",
    )
    _write(out_dir, store_aggregate, time_aggregate, summary)
    return summary


def run_chunks(
    interactions: Path,
    catalog_path: Path,
    out_dir: Path,
    chunksize: int = 250_000,
    max_rows: int | None = None,
) -> dict:
    """Mitigation 3: stream the raw JSONL in chunks with partial aggregation.

    Each chunk is cleaned, derived and mapped to its store, and only the
    partial sums are kept. What cannot be reduced to sums is the distinct
    product count per store (``products``): it needs the set of ASINs per
    store alive until the end, which is exactly the memory the chunking was
    trying to avoid. That set is kept here so the output still matches the
    contract, and its cost is measured and reported.
    """
    started = time.perf_counter()
    store_map = analysis.load_catalog(catalog_path).set_index("parent_asin")["store"]

    store_stats: dict = {}   # store -> [count, sum_rating, sumsq_rating, sum_textlen]
    store_asins: dict = {}   # store -> set of parent_asin (for products)
    time_stats: dict = {}    # (year, month, dow) -> [count, sum_rating, sumsq_rating, sum_textlen]

    def _accumulate(frame: pd.DataFrame) -> None:
        for (store,), group in frame.groupby(["store"], dropna=False, sort=False):
            # None instead of NaN: dict keys must be stable across chunks.
            key = store if pd.notna(store) else None
            stats = store_stats.setdefault(key, [0, 0.0, 0.0, 0.0])
            ratings = group["rating"].to_numpy(dtype="float64")
            stats[0] += len(group)
            stats[1] += float(ratings.sum())
            stats[2] += float((ratings * ratings).sum())
            stats[3] += float(group["text_length"].sum())
            asins = store_asins.setdefault(key, set())
            asins.update(group["parent_asin"].tolist())
        for keys, group in frame.groupby(
            ["review_year", "review_month", "review_dow"], sort=False
        ):
            stats = time_stats.setdefault(tuple(int(k) for k in keys), [0, 0.0, 0.0, 0.0])
            ratings = group["rating"].to_numpy(dtype="float64")
            stats[0] += len(group)
            stats[1] += float(ratings.sum())
            stats[2] += float((ratings * ratings).sum())
            stats[3] += float(group["text_length"].sum())

    rows = 0
    for chunk in pd.read_json(
        interactions, lines=True, chunksize=chunksize, convert_dates=False
    ):
        chunk = chunk[ANALYSIS_COLUMNS]
        chunk = analysis.add_derived_columns(analysis.clean_reviews(chunk))
        chunk["store"] = chunk["parent_asin"].map(store_map)
        _accumulate(chunk)
        rows += len(chunk)
        if max_rows is not None and rows >= max_rows:
            break

    def _table(stats: dict, keys: list[str], with_products: bool) -> pd.DataFrame:
        records = []
        for key, (count, total, sumsq, textlen) in stats.items():
            avg = total / count if count else 0.0
            # NaN for a single-row group: matches pandas std and Spark stddev.
            if count > 1:
                variance = (sumsq - total * total / count) / (count - 1)
                stddev = round(math.sqrt(max(variance, 0.0)), 4)
            else:
                stddev = float("nan")
            record = {
                **dict(zip(keys, key if isinstance(key, tuple) else (key,))),
                "reviews": count,
                "avg_rating": round(avg, 4),
                "stddev_rating": stddev,
                "avg_text_length": round(textlen / count, 4) if count else 0.0,
            }
            if with_products:
                record = {
                    "store": record["store"],
                    "reviews": record["reviews"],
                    "products": len(store_asins.get(record["store"], ())),
                    "avg_rating": record["avg_rating"],
                    "stddev_rating": record["stddev_rating"],
                    "avg_text_length": record["avg_text_length"],
                }
            records.append(record)
        columns = (
            ["store", "reviews", "products", "avg_rating", "stddev_rating", "avg_text_length"]
            if with_products
            else keys + ["reviews", "avg_rating", "stddev_rating", "avg_text_length"]
        )
        return pd.DataFrame(records, columns=columns)

    store_aggregate = _table(store_stats, ["store"], with_products=True)
    time_aggregate = _table(
        time_stats, ["review_year", "review_month", "review_dow"], with_products=False
    )
    asin_count = sum(len(asins) for asins in store_asins.values())
    summary = _summary(
        "chunks", {"asin_sets": round(
            sum(len(s) * 120 for s in store_asins.values()) / (1024**2), 1
        )}, rows,
        time.perf_counter() - started, out_dir,
        notes=(
            f"exact products needs {asin_count:,} ASINs alive as sets per store; "
            "global operations (sort, percentiles, windows) cannot be chunked"
        ),
    )
    _write(out_dir, store_aggregate, time_aggregate, summary)
    return summary


TECHNIQUES = {
    "columns": run_column_selection,
    "dtypes": run_optimized_dtypes,
    "chunks": run_chunks,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one pandas mitigation technique and record its cost."
    )
    parser.add_argument("--technique", choices=sorted(TECHNIQUES), required=True)
    parser.add_argument("--interactions", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--chunksize", type=int, default=250_000)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.technique == "chunks":
        summary = run_chunks(
            Path(args.interactions), Path(args.catalog), out_dir,
            chunksize=args.chunksize, max_rows=args.max_rows,
        )
    else:
        summary = TECHNIQUES[args.technique](
            Path(args.interactions), Path(args.catalog), out_dir
        )
    print(f"[mitigated] {json.dumps(summary, ensure_ascii=True)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
