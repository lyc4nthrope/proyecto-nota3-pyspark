"""Smoke test: proves Java + Spark + PySpark are correctly installed.

Run it first on any new machine (Linux or Windows): `pytest -q`.
If it fails, nothing else in the project can work.
"""

import pytest

from spark_version.session import build_spark_session


@pytest.fixture(scope="module")
def spark():
    """Start one real SparkSession for the module and stop it at the end."""
    session = build_spark_session(app_name="smoke-test", driver_memory="1g")
    yield session
    session.stop()


def test_spark_runs_a_distributed_count(spark):
    # count() is an action: it forces Spark to actually execute a job.
    assert spark.range(10).count() == 10


def test_session_uses_all_cores_in_local_mode(spark):
    assert spark.sparkContext.master == "local[*]"


def test_session_time_zone_is_fixed_to_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_spark_can_write_and_read_parquet(spark, tmp_path):
    # Parquet is the output format of the project; on Windows this is the
    # step that fails when winutils.exe / HADOOP_HOME are missing.
    target = str(tmp_path / "numbers.parquet")
    spark.range(100).write.parquet(target)
    assert spark.read.parquet(target).count() == 100
