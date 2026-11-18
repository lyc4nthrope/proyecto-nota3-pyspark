"""Mitigation techniques must keep the contract's outputs.

The chunked version is the risky one: it reduces each chunk to partial sums,
and a distinct product count per store cannot be reduced that way. These tests
pin that the chunked aggregates still equal the standard ones, including a
null store, a single-row group (stddev = NaN) and chunks that split groups.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

import pandas_version.analysis as analysis
from pandas_version.mitigated import (
    run_chunks,
    run_column_selection,
    run_optimized_dtypes,
)

SUNDAY_MS = 1615120496000

ROWS = [
    {"user_id": "u1", "parent_asin": "p1", "rating": 5.0, "timestamp": SUNDAY_MS, "text": "great"},
    {"user_id": "u2", "parent_asin": "p1", "rating": 3.0, "timestamp": SUNDAY_MS, "text": "ok"},
    {"user_id": "u3", "parent_asin": "p2", "rating": 4.0, "timestamp": SUNDAY_MS, "text": "fine"},
    {"user_id": "u4", "parent_asin": "p9", "rating": 2.0, "timestamp": SUNDAY_MS, "text": "no catalog"},
    {"user_id": "u5", "parent_asin": "p1", "rating": 4.0, "timestamp": SUNDAY_MS, "text": None},
    {"user_id": "u6", "parent_asin": "p2", "rating": 1.0, "timestamp": SUNDAY_MS + 86_400_000, "text": "meh"},
]

CATALOG_ROWS = [
    {"parent_asin": "p1", "main_category": "Kitchen", "store": "ACME",
     "price": 9.99, "average_rating": 4.5, "rating_number": 3},
    {"parent_asin": "p2", "main_category": "Home", "store": "BEST",
     "price": 19.99, "average_rating": 2.5, "rating_number": 2},
]


def _standard_tables(frame: pd.DataFrame, catalog: pd.DataFrame):
    derived = analysis.add_derived_columns(analysis.clean_reviews(frame))
    joined = analysis.join_with_catalog(derived, catalog)
    return analysis.aggregate_by_store(joined), analysis.aggregate_by_time(joined)


def _assert_tables_equal(actual: pd.DataFrame, expected: pd.DataFrame, keys: list[str]) -> None:
    left = actual.copy()
    right = expected.copy()
    for frame in (left, right):
        for key in keys:
            frame[key] = frame[key].astype("object").where(frame[key].notna(), "(none)")
    left = left.sort_values(keys).reset_index(drop=True)
    right = right.sort_values(keys).reset_index(drop=True)
    for key in keys:
        assert left[key].tolist() == right[key].tolist()
    assert left["reviews"].tolist() == right["reviews"].tolist()
    for column in ("avg_rating", "stddev_rating", "avg_text_length"):
        np.testing.assert_allclose(
            left[column].to_numpy(dtype="float64"),
            right[column].to_numpy(dtype="float64"),
            rtol=0, atol=1e-4, equal_nan=True,
        )


def _fixtures(tmp_path):
    interactions = tmp_path / "rows.jsonl"
    with interactions.open("w", encoding="utf-8") as handle:
        for row in ROWS:
            handle.write(json.dumps(row) + "\n")
    parquet = tmp_path / "rows.parquet"
    pd.DataFrame(ROWS).to_parquet(parquet, index=False)
    catalog_path = tmp_path / "catalog.parquet"
    pd.DataFrame(CATALOG_ROWS).to_parquet(catalog_path, index=False)
    return interactions, parquet, catalog_path


def test_chunks_match_the_standard_aggregation(tmp_path):
    interactions, _, catalog_path = _fixtures(tmp_path)
    # chunksize=2 forces groups to be split across several partial accumulations.
    summary = run_chunks(
        interactions, catalog_path, tmp_path / "out", chunksize=2
    )

    store = pd.read_parquet(tmp_path / "out" / "agg_by_store.parquet")
    time_table = pd.read_parquet(tmp_path / "out" / "agg_by_time.parquet")
    expected_store, expected_time = _standard_tables(
        pd.DataFrame(ROWS), analysis.load_catalog(catalog_path)
    )

    _assert_tables_equal(store, expected_store, ["store"])
    _assert_tables_equal(
        time_table, expected_time, ["review_year", "review_month", "review_dow"]
    )
    left = store.copy()
    left["store"] = left["store"].astype("object").where(left["store"].notna(), "(none)")
    right = expected_store.copy()
    right["store"] = right["store"].astype("object").where(right["store"].notna(), "(none)")
    left = left.sort_values("store").reset_index(drop=True)
    right = right.sort_values("store").reset_index(drop=True)
    assert left["products"].tolist() == right["products"].tolist()
    assert summary["rows"] == len(ROWS)
    assert "ASINs alive as sets per store" in summary["notes"]


def test_chunks_respects_max_rows(tmp_path):
    interactions, _, catalog_path = _fixtures(tmp_path)
    summary = run_chunks(
        interactions, catalog_path, tmp_path / "out", chunksize=2, max_rows=3
    )
    assert summary["rows"] == 4  # stops after the chunk that crosses the limit


def test_column_selection_keeps_the_contract_tables(tmp_path):
    _, parquet, catalog_path = _fixtures(tmp_path)
    summary = run_column_selection(parquet, catalog_path, tmp_path / "out")

    store = pd.read_parquet(tmp_path / "out" / "agg_by_store.parquet")
    expected_store, _ = _standard_tables(
        pd.DataFrame(ROWS), analysis.load_catalog(catalog_path)
    )
    _assert_tables_equal(store, expected_store, ["store"])
    assert summary["technique"] == "columns"
    assert summary["rows"] == len(ROWS)


def test_optimized_dtypes_keep_the_contract_tables(tmp_path):
    _, parquet, catalog_path = _fixtures(tmp_path)
    summary = run_optimized_dtypes(parquet, catalog_path, tmp_path / "out")

    store = pd.read_parquet(tmp_path / "out" / "agg_by_store.parquet")
    expected_store, _ = _standard_tables(
        pd.DataFrame(ROWS), analysis.load_catalog(catalog_path)
    )
    _assert_tables_equal(store, expected_store, ["store"])
    assert summary["technique"] == "dtypes"
