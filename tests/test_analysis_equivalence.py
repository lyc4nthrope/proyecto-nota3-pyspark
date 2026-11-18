"""The pandas and the Spark versions must produce the same analysis outputs.

This is the guard behind the project's equivalence requirement: same cleaning
rules, same derived columns (including Spark's 1=Sunday weekday numbering) and
same aggregates, on the same input. It also pins the deterministic hash filter
that builds the 1/10/50/100% ladder samples.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from benchmark import ladder
from pandas_version import analysis as pandas_analysis
from spark_version import analysis as spark_analysis
from spark_version.session import build_spark_session

# 2021-03-07 12:34:56 UTC is a Sunday: Spark's dayofweek for it is 1.
SUNDAY_MS = 1615120496000

REVIEW_ROWS = [
    {"user_id": "u1", "parent_asin": "p1", "rating": 5.0, "timestamp": SUNDAY_MS},
    {"user_id": "u2", "parent_asin": "p1", "rating": 3.0, "timestamp": SUNDAY_MS},
    {"user_id": "u3", "parent_asin": "p2", "rating": 4.0, "timestamp": SUNDAY_MS + 3_600_000},
    {"user_id": "u4", "parent_asin": "p3", "rating": 2.0, "timestamp": SUNDAY_MS + 7_200_000},
    # These three must be dropped by the cleaning rules.
    {"user_id": "u5", "parent_asin": "p2", "rating": 6.0, "timestamp": SUNDAY_MS},
    {"user_id": "u6", "parent_asin": "p2", "rating": 4.0, "timestamp": 0},
    {"user_id": None, "parent_asin": "p2", "rating": 4.0, "timestamp": SUNDAY_MS},
]

CATALOG_ROWS = [
    {"parent_asin": "p1", "main_category": "Kitchen", "store": "ACME",
     "price": 9.99, "average_rating": 4.5, "rating_number": 2},
    {"parent_asin": "p2", "main_category": "Home", "store": "ACME",
     "price": 19.99, "average_rating": 3.5, "rating_number": 1},
    {"parent_asin": "p3", "main_category": "Home", "store": "BEST",
     "price": 4.99, "average_rating": 2.0, "rating_number": 1},
]


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session(app_name="equivalence-test", driver_memory="1g")
    yield session
    session.stop()


def _pandas_path(tmp_path):
    reviews = pd.DataFrame(REVIEW_ROWS)
    catalog = pd.DataFrame(CATALOG_ROWS)
    catalog_path = tmp_path / "catalog.parquet"
    catalog.to_parquet(catalog_path, index=False)
    cleaned = pandas_analysis.clean_reviews(reviews)
    derived = pandas_analysis.add_derived_columns(cleaned)
    joined = pandas_analysis.join_with_catalog(
        derived, pandas_analysis.load_catalog(catalog_path)
    )
    return derived, joined


def _spark_path(spark, tmp_path):
    reviews = spark.createDataFrame(pd.DataFrame(REVIEW_ROWS), schema=spark_analysis.REVIEW_SCHEMA)
    catalog = spark.createDataFrame(pd.DataFrame(CATALOG_ROWS), schema=spark_analysis.META_SCHEMA)
    catalog_path = tmp_path / "catalog_spark.parquet"
    catalog.select(*spark_analysis.CATALOG_COLUMNS).write.mode(
        "overwrite"
    ).parquet(str(catalog_path))
    cleaned = spark_analysis.clean_reviews(reviews)
    derived = spark_analysis.add_derived_columns(cleaned)
    joined = spark_analysis.join_with_catalog(
        derived, spark_analysis.load_catalog(spark, catalog_path)
    )
    return derived, joined


def test_cleaning_rules_drop_the_same_rows_in_both_engines(spark, tmp_path):
    pandas_derived, _ = _pandas_path(tmp_path)
    spark_derived, _ = _spark_path(spark, tmp_path)
    assert len(pandas_derived) == 4
    assert spark_derived.count() == 4


def _assert_close(obtained, expected, atol: float = 1e-4) -> None:
    """NaN-aware float comparison: both engines return null/NaN for a
    single-row standard deviation, which is the same semantic value."""
    np.testing.assert_allclose(
        np.asarray(obtained, dtype="float64"),
        np.asarray(expected, dtype="float64"),
        rtol=0,
        atol=atol,
        equal_nan=True,
    )


def test_sunday_maps_to_spark_weekday_number_one(spark, tmp_path):
    pandas_derived, _ = _pandas_path(tmp_path)
    spark_derived, _ = _spark_path(spark, tmp_path)
    # Every kept row is on Sunday, so the only weekday number is Spark's 1.
    assert set(pandas_derived["review_dow"].tolist()) == {1}
    spark_dows = {row["review_dow"] for row in spark_derived.select("review_dow").collect()}
    assert spark_dows == set(pandas_derived["review_dow"].tolist())


def test_weekday_numbering_matches_spark_for_all_days(spark):
    # The week of Monday 2021-03-08 UTC: one row per weekday.
    monday_ms = 1615161600000
    rows = [
        {"user_id": f"u{i}", "parent_asin": "p1", "rating": 4.0,
         "timestamp": monday_ms + i * 86_400_000}
        for i in range(7)
    ]
    frame = pd.DataFrame(rows)
    pandas_derived = pandas_analysis.add_derived_columns(
        pandas_analysis.clean_reviews(frame)
    )
    spark_derived = spark_analysis.add_derived_columns(
        spark_analysis.clean_reviews(
            spark.createDataFrame(frame, schema=spark_analysis.REVIEW_SCHEMA)
        )
    )
    spark_dows = [
        row["review_dow"]
        for row in spark_derived.select("review_dow").orderBy("timestamp").collect()
    ]
    assert pandas_derived["review_dow"].tolist() == spark_dows
    assert sorted(spark_dows) == [1, 2, 3, 4, 5, 6, 7]


def test_join_coverage_and_unmatched_counts_match(spark, tmp_path):
    _, pandas_joined = _pandas_path(tmp_path)
    _, spark_joined = _spark_path(spark, tmp_path)
    assert len(pandas_joined) == spark_joined.count() == 4
    pandas_unmatched = int(pandas_joined["_meta_matched"].isna().sum())
    spark_unmatched = spark_joined.filter(
        spark_joined["_meta_matched"].isNull()
    ).count()
    assert pandas_unmatched == spark_unmatched == 0


def test_store_aggregates_match(spark, tmp_path):
    _, pandas_joined = _pandas_path(tmp_path)
    _, spark_joined = _spark_path(spark, tmp_path)
    pandas_aggregate = pandas_analysis.aggregate_by_store(pandas_joined).sort_values(
        ["store"]
    ).reset_index(drop=True)
    spark_aggregate = (
        spark_analysis.aggregate_by_store(spark_joined)
        .orderBy("store")
        .toPandas()
        .reset_index(drop=True)
    )
    assert pandas_aggregate["store"].tolist() == spark_aggregate["store"].tolist()
    assert pandas_aggregate["reviews"].tolist() == spark_aggregate["reviews"].tolist()
    assert pandas_aggregate["products"].tolist() == spark_aggregate["products"].tolist()
    _assert_close(pandas_aggregate["avg_rating"], spark_aggregate["avg_rating"])
    _assert_close(
        pandas_aggregate["stddev_rating"], spark_aggregate["stddev_rating"]
    )


def test_time_aggregates_match(spark, tmp_path):
    _, pandas_joined = _pandas_path(tmp_path)
    _, spark_joined = _spark_path(spark, tmp_path)
    keys = ["review_year", "review_month", "review_dow"]
    pandas_aggregate = pandas_analysis.aggregate_by_time(pandas_joined).sort_values(
        keys
    ).reset_index(drop=True)
    spark_aggregate = (
        spark_analysis.aggregate_by_time(spark_joined)
        .orderBy(*keys)
        .toPandas()
        .reset_index(drop=True)
    )
    for key in keys:
        assert pandas_aggregate[key].tolist() == spark_aggregate[key].tolist()
    assert pandas_aggregate["reviews"].tolist() == spark_aggregate["reviews"].tolist()
    _assert_close(pandas_aggregate["avg_rating"], spark_aggregate["avg_rating"])


def test_ladder_hash_filter_is_deterministic_and_scales(spark):
    condition = ladder.hash_fraction_condition(0.10)
    assert condition is not None
    assert ladder.hash_fraction_condition(1.0) is None
    with pytest.raises(ValueError):
        ladder.hash_fraction_condition(0.0)
    with pytest.raises(ValueError):
        ladder.hash_fraction_condition(1.5)
    assert ladder.level_tag(0.01) == "1pct"
    assert ladder.level_tag(1.0) == "100pct"

    rows = [
        {"user_id": f"u{i}", "parent_asin": f"p{i % 7}", "rating": float(i % 5 + 1),
         "timestamp": SUNDAY_MS + i * 1000}
        for i in range(2000)
    ]
    frame = spark.createDataFrame(pd.DataFrame(rows), schema=spark_analysis.REVIEW_SCHEMA)
    first = frame.filter(condition).count()
    second = frame.filter(condition).count()
    assert first == second
    assert 0 < first < 2000
