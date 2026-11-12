"""Tests for the measurement script.

They launch real child processes (no mocks): the guarantees under test are
about what the operating system reports for time and memory.
"""

import csv
import sys

import pytest

from benchmark.measure import (
    RESULT_COLUMNS,
    append_result,
    build_command,
    collect_environment,
    measure,
)

MB = 1024 * 1024
POLL = 0.05


def python_command(code: str) -> list[str]:
    """Build a command that runs a short Python snippet in a child process."""
    return [sys.executable, "-c", code]


def test_wall_time_covers_the_whole_run():
    result = measure(python_command("import time; time.sleep(0.6)"), poll_seconds=POLL)

    assert result.status == "ok"
    assert result.exit_code == 0
    assert 0.6 <= result.wall_seconds < 3


def test_peak_memory_sees_what_the_process_allocates():
    code = "import time; block = bytearray(300 * 1024 * 1024); time.sleep(0.8)"

    result = measure(python_command(code), poll_seconds=POLL)

    assert 300 <= result.peak_rss_mb < 700


def test_peak_memory_is_the_highest_reading_not_the_last_one():
    # pandas frees memory before exiting: the last sample is lower than the peak.
    code = (
        "import time; block = bytearray(300 * 1024 * 1024); time.sleep(0.6); "
        "del block; time.sleep(0.8)"
    )

    result = measure(python_command(code), poll_seconds=POLL)

    assert result.peak_rss_mb >= 300


def test_peak_memory_includes_child_processes():
    # Spark runs inside a JVM started by Python: memory lives in a child process.
    child = "import time; block = bytearray(250 * 1024 * 1024); time.sleep(0.8)"
    parent = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {child!r}])"

    result = measure(python_command(parent), poll_seconds=POLL)

    assert result.peak_rss_mb >= 250


def test_cpu_time_is_recorded_for_busy_processes():
    code = "import time; end = time.time() + 0.8\nwhile time.time() < end: pass"

    result = measure(python_command(code), poll_seconds=POLL)

    assert result.cpu_seconds >= 0.4


def test_failure_is_reported_with_its_exit_code():
    result = measure(python_command("import sys; sys.exit(7)"), poll_seconds=POLL)

    assert result.status == "failed"
    assert result.exit_code == 7


def test_run_is_stopped_when_it_exceeds_the_time_limit():
    result = measure(
        python_command("import time; time.sleep(30)"),
        timeout_seconds=0.5,
        poll_seconds=POLL,
    )

    assert result.status == "timeout"
    assert result.wall_seconds < 5


def test_time_limit_also_stops_child_processes(tmp_path):
    # If the JVM survived a timeout it would keep using RAM during the next run.
    marker = tmp_path / "child_finished.txt"
    child = f"import time, pathlib; time.sleep(2); pathlib.Path({str(marker)!r}).write_text('x')"
    parent = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {child!r}])"

    result = measure(python_command(parent), timeout_seconds=0.5, poll_seconds=POLL)

    import time

    time.sleep(2.5)
    assert result.status == "timeout"
    assert not marker.exists()


def test_command_follows_the_analysis_contract():
    command = build_command("spark", "pct_010")

    assert command == [
        sys.executable,
        "-m",
        "spark_version.run",
        "--input",
        "data/samples/pct_010",
        "--zones",
        "data/raw/taxi_zone_lookup.csv",
        "--output",
        "output/spark/pct_010",
    ]


def test_unknown_engine_is_rejected():
    with pytest.raises(ValueError):
        build_command("polars", "pct_010")


def test_results_are_appended_with_a_single_header(tmp_path):
    results_file = tmp_path / "nested" / "results.csv"
    first = measure(python_command("pass"), poll_seconds=POLL)
    second = measure(python_command("import sys; sys.exit(3)"), poll_seconds=POLL)

    append_result(results_file, engine="pandas", sample="pct_001", repeat=1, result=first)
    append_result(results_file, engine="pandas", sample="pct_001", repeat=2, result=second)

    with results_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert list(rows[0].keys()) == RESULT_COLUMNS
    assert [row["repeat"] for row in rows] == ["1", "2"]
    assert [row["status"] for row in rows] == ["ok", "failed"]
    assert rows[1]["exit_code"] == "3"


def test_environment_describes_the_test_machine():
    environment = collect_environment()

    assert environment["cpu_logical_cores"] >= 1
    assert environment["ram_total_gb"] > 0
    assert environment["python_version"].startswith(f"{sys.version_info.major}.")
    for key in ("os", "cpu_model", "disk_free_gb", "pandas_version", "pyspark_version"):
        assert environment[key] not in ("", None)
