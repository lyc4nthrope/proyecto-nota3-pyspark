"""The optimized Spark version must give the baseline's results with a better plan.

Equivalence is checked against the baseline Spark functions, which
test_analysis_equivalence.py already proves equal to pandas.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from spark_version import analysis, optimized
from spark_version.optimized import OptimizationOptions
from spark_version.session import build_spark_session

SUNDAY_MS = 1615120496000  # 2021-03-07 12:34:56 UTC

REVIEWS = pd.DataFrame(
    [
        {"user_id": "u1", "parent_asin": "p1", "rating": 5.0, "timestamp": SUNDAY_MS},
        {"user_id": "u2", "parent_asin": "p1", "rating": 3.0, "timestamp": SUNDAY_MS},
        {"user_id": "u3", "parent_asin": "p2", "rating": 4.0, "timestamp": SUNDAY_MS + 3_600_000},
        {"user_id": "u4", "parent_asin": "p3", "rating": 2.0, "timestamp": SUNDAY_MS + 86_400_000},
        {"user_id": "u7", "parent_asin": "p9", "rating": 1.0, "timestamp": SUNDAY_MS},  # not in catalog
        {"user_id": "u5", "parent_asin": "p2", "rating": 6.0, "timestamp": SUNDAY_MS},  # dropped
        {"user_id": "u6", "parent_asin": "p2", "rating": 4.0, "timestamp": 0},  # dropped
    ]
)
CATALOG = pd.DataFrame(
    [
        {"parent_asin": "p1", "main_category": "Kitchen", "store": "ACME",
         "price": 9.99, "average_rating": 4.5, "rating_number": 2},
        {"parent_asin": "p2", "main_category": "Home", "store": "ACME",
         "price": 19.99, "average_rating": 3.5, "rating_number": 1},
        {"parent_asin": "p3", "main_category": "Home", "store": "BEST",
         "price": 4.99, "average_rating": 2.0, "rating_number": 1},
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session(app_name="optimized-test", driver_memory="1g")
    yield session
    session.stop()


@pytest.fixture()
def inputs(tmp_path):
    """Write the synthetic reviews and catalog as Parquet, like the real samples."""
    reviews_path = tmp_path / "reviews.parquet"
    catalog_path = tmp_path / "catalog.parquet"
    REVIEWS.to_parquet(reviews_path, index=False)
    CATALOG.to_parquet(catalog_path, index=False)
    return reviews_path, catalog_path


def _baseline_tables(spark, reviews_path, catalog_path, tables_dir):
    """Run the unoptimized functions exactly as the baseline does, and read its tables."""
    reviews = spark.read.schema(analysis.REVIEW_SCHEMA).parquet(str(reviews_path))
    derived = analysis.add_derived_columns(analysis.clean_reviews(reviews))
    joined = analysis.join_with_catalog(derived, analysis.load_catalog(spark, catalog_path))
    analysis.write_outputs(
        analysis.aggregate_by_store(joined), analysis.aggregate_by_time(joined), tables_dir
    )
    return (
        pd.read_parquet(tables_dir / "agg_by_store.parquet"),
        pd.read_parquet(tables_dir / "agg_by_time.parquet"),
    )


def _sorted(frame, keys):
    return frame.sort_values(keys, na_position="first").reset_index(drop=True)


@pytest.mark.parametrize(
    "options",
    [
        OptimizationOptions(),
        OptimizationOptions(cache_base=True),
        OptimizationOptions(broadcast_catalog=False, cache_base=False, shuffle_partitions=3),
    ],
    ids=["defaults", "with-cache", "none"],
)
def test_optimized_tables_equal_the_baseline(spark, inputs, tmp_path, options):
    reviews_path, catalog_path = inputs
    expected_store, expected_time = _baseline_tables(
        spark, reviews_path, catalog_path, tmp_path / "baseline"
    )

    counts = optimized.run_optimized(spark, reviews_path, catalog_path, tmp_path / "out", options)

    store = pd.read_parquet(tmp_path / "out" / "agg_by_store.parquet")
    by_time = pd.read_parquet(tmp_path / "out" / "agg_by_time.parquet")
    assert list(store.columns) == list(expected_store.columns)
    assert list(by_time.columns) == list(expected_time.columns)
    pd.testing.assert_frame_equal(
        _sorted(store, ["store"]), _sorted(expected_store, ["store"]), check_dtype=False
    )
    time_keys = ["review_year", "review_month", "review_dow"]
    pd.testing.assert_frame_equal(
        _sorted(by_time, time_keys), _sorted(expected_time, time_keys), check_dtype=False
    )
    assert counts == {"cleaned_rows": 5, "store_rows": 3, "time_rows": 2}


def test_review_without_catalog_is_kept_with_null_store(spark, inputs, tmp_path):
    reviews_path, catalog_path = inputs

    optimized.run_optimized(spark, reviews_path, catalog_path, tmp_path / "out")

    store = pd.read_parquet(tmp_path / "out" / "agg_by_store.parquet")
    assert int(store.loc[store["store"].isna(), "reviews"].iloc[0]) == 1


def test_store_plan_uses_broadcast_join(spark, inputs):
    reviews_path, catalog_path = inputs
    options = OptimizationOptions(cache_base=False)
    # Turn off automatic broadcasting: the tiny test catalog would be broadcast
    # anyway, so only the explicit hint may produce a BroadcastHashJoin here.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        base = optimized.build_base(optimized.read_reviews(spark, reviews_path), options)
        plan = optimized.physical_plan(
            optimized.store_aggregate(base, optimized.read_catalog_for_join(spark, catalog_path), options)
        )
    finally:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

    assert "BroadcastHashJoin" in plan
    assert "SortMergeJoin" not in plan


def test_store_plan_without_broadcast_uses_sort_merge_join(spark, inputs):
    reviews_path, catalog_path = inputs
    options = OptimizationOptions(broadcast_catalog=False, cache_base=False)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        base = optimized.build_base(optimized.read_reviews(spark, reviews_path), options)
        plan = optimized.physical_plan(
            optimized.store_aggregate(base, optimized.read_catalog_for_join(spark, catalog_path), options)
        )
    finally:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

    assert "SortMergeJoin" in plan


def test_time_plan_has_no_join(spark, inputs):
    reviews_path, _ = inputs
    base = optimized.build_base(
        optimized.read_reviews(spark, reviews_path), OptimizationOptions(cache_base=False)
    )

    assert "Join" not in optimized.physical_plan(optimized.time_aggregate(base))


def test_reviews_are_read_with_only_the_needed_columns(spark, inputs):
    reviews_path, _ = inputs
    wide = REVIEWS.assign(text="long review text", title="t")
    wide.to_parquet(reviews_path, index=False)

    plan = optimized.physical_plan(optimized.read_reviews(spark, reviews_path))

    read_schema = next(line for line in plan.splitlines() if "ReadSchema" in line)
    assert "text" not in read_schema and "title" not in read_schema


def test_cache_is_used_when_enabled(spark, inputs):
    reviews_path, _ = inputs
    base = optimized.build_base(
        optimized.read_reviews(spark, reviews_path), OptimizationOptions(cache_base=True)
    )

    assert base.storageLevel.useMemory and base.storageLevel.useDisk
    base.unpersist()


def test_cache_is_off_by_default(spark, inputs):
    reviews_path, _ = inputs
    base = optimized.build_base(optimized.read_reviews(spark, reviews_path), OptimizationOptions())

    assert not base.storageLevel.useMemory


def test_shuffle_partitions_option_is_applied(spark, inputs, tmp_path):
    reviews_path, catalog_path = inputs

    optimized.run_optimized(
        spark, reviews_path, catalog_path, tmp_path / "out", OptimizationOptions(shuffle_partitions=7)
    )

    assert spark.conf.get("spark.sql.shuffle.partitions") == "7"


def test_explain_file_records_both_plans(spark, inputs, tmp_path):
    reviews_path, catalog_path = inputs
    plan_file = tmp_path / "plan.txt"

    optimized.run_optimized(spark, reviews_path, catalog_path, tmp_path / "out", explain_path=plan_file)

    text = plan_file.read_text(encoding="utf-8")
    store_plan, time_plan = text.split("== agg_by_time ==")
    assert "== agg_by_store ==" in store_plan
    assert "BroadcastHashJoin" in store_plan
    # The pipeline must compute the temporal table without any join.
    assert "Join" not in time_plan


def test_each_output_table_is_written_as_a_single_file(spark, inputs, tmp_path):
    reviews_path, catalog_path = inputs
    # Adaptive execution would merge the small shuffle partitions by itself;
    # turning it off leaves one file per non-empty partition unless we coalesce.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    try:
        optimized.run_optimized(
            spark, reviews_path, catalog_path, tmp_path / "out", OptimizationOptions(shuffle_partitions=7)
        )
    finally:
        spark.conf.unset("spark.sql.adaptive.enabled")

    for table in ("agg_by_store.parquet", "agg_by_time.parquet"):
        parts = list((tmp_path / "out" / table).glob("part-*.parquet"))
        assert len(parts) == 1, table


def test_command_line_runs_end_to_end(inputs, tmp_path, capsys):
    from spark_version import run

    reviews_path, catalog_path = inputs
    exit_code = run.main(
        ["--reviews", str(reviews_path), "--catalog", str(catalog_path),
         "--output", str(tmp_path / "cli"), "--driver-memory", "1g"]
    )

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert exit_code == 0
    assert summary["cleaned_rows"] == 5 and summary["broadcast"] is True
    assert summary["cache"] is False
    assert (tmp_path / "cli" / "agg_by_store.parquet").exists()
