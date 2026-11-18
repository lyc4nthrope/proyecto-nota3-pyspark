"""The pandas reference pipeline must follow the contract exactly.

Covers the per-stage row counts (the numbers later validated by logical
analysis) and the summary invariants: the three rule counts must add up to the
rows dropped, and a left join must never change the row count.
"""

from __future__ import annotations

import json

import pandas as pd

from pandas_version.pipeline import run_pipeline

# 2 valid rows, plus one row per cleaning rule and one duplicate.
SUNDAY_MS = 1615120496000
ROWS = [
    {"user_id": "u1", "parent_asin": "p1", "rating": 5.0, "timestamp": SUNDAY_MS},
    {"user_id": "u2", "parent_asin": "p2", "rating": 3.0, "timestamp": SUNDAY_MS},
    {"user_id": "u1", "parent_asin": "p1", "rating": 5.0, "timestamp": SUNDAY_MS},  # exact duplicate of row 1
    {"user_id": None, "parent_asin": "p1", "rating": 4.0, "timestamp": SUNDAY_MS},  # null
    {"user_id": "u5", "parent_asin": "p2", "rating": 6.0, "timestamp": SUNDAY_MS},  # rating out of range
    {"user_id": "u6", "parent_asin": "p2", "rating": 4.0, "timestamp": 0},          # invalid timestamp
]

CATALOG_ROWS = [
    {"parent_asin": "p1", "main_category": "Kitchen", "store": "ACME",
     "price": 9.99, "average_rating": 4.5, "rating_number": 2},
    {"parent_asin": "p2", "main_category": "Home", "store": "BEST",
     "price": 19.99, "average_rating": 3.5, "rating_number": 1},
]


def test_pipeline_follows_the_contract_stage_counts(tmp_path):
    interactions = tmp_path / "interactions.parquet"
    catalog = tmp_path / "catalog.parquet"
    output = tmp_path / "out"
    pd.DataFrame(ROWS).to_parquet(interactions, index=False)
    pd.DataFrame(CATALOG_ROWS).to_parquet(catalog, index=False)

    summary = run_pipeline(interactions, catalog, output)

    assert summary["input_rows"] == 6
    assert summary["dropped_nulls"] == 1
    assert summary["dropped_rating"] == 1
    assert summary["dropped_timestamp"] == 1
    assert summary["clean_rows"] == 3
    # A left join never changes the row count.
    assert summary["joined_rows"] == summary["clean_rows"]
    assert summary["unmatched_rows"] == 0
    assert summary["store_groups"] == 2

    dropped = (
        summary["dropped_nulls"]
        + summary["dropped_rating"]
        + summary["dropped_timestamp"]
    )
    assert summary["input_rows"] - dropped == summary["clean_rows"]

    assert (output / "agg_by_store.parquet").exists()
    assert (output / "agg_by_time.parquet").exists()
    written = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert written["engine"] == "pandas"

    store_table = pd.read_parquet(output / "agg_by_store.parquet")
    assert list(store_table.columns) == [
        "store", "reviews", "products", "avg_rating", "stddev_rating",
        "avg_text_length",
    ]
    assert int(store_table["reviews"].sum()) == summary["joined_rows"]
