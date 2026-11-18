"""The guarded runner must kill a run before it freezes the machine.

A pandas that starts paging can hold the whole system hostage long before its
timeout fires. These tests pin the two escape hatches: the wall-clock timeout
and the system memory limit.
"""

from __future__ import annotations

import sys
import time
from types import SimpleNamespace

from benchmark.instrument import measure_operation, run_with_limits


def test_timeout_kills_the_child_and_records_the_reason(tmp_path):
    outcome = run_with_limits(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        timeout_s=1,
        log_path=tmp_path / "child.log",
        poll_s=0.1,
    )

    assert outcome["kill_reason"] == "timeout after 1s"
    assert outcome["returncode"] != 0
    # The runaway process' own memory is part of the recorded outcome.
    assert outcome["tree_mb_at_kill"] is not None
    assert outcome["tree_mb_at_kill"] > 0


def test_memory_limit_kills_the_child_and_records_the_reason(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        "benchmark.instrument.psutil.virtual_memory",
        lambda: SimpleNamespace(percent=97.0, available=100 * 1024**2),
    )
    outcome = run_with_limits(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        timeout_s=30,
        log_path=tmp_path / "child.log",
        poll_s=0.1,
    )

    assert outcome["kill_reason"].startswith("killed at system RAM 97%")
    assert outcome["returncode"] != 0


def test_child_that_finishes_is_not_killed(tmp_path):
    outcome = run_with_limits(
        [sys.executable, "-c", "print('[child] ok')"],
        timeout_s=30,
        log_path=tmp_path / "child.log",
        poll_s=0.1,
    )

    assert outcome["kill_reason"] is None
    assert outcome["returncode"] == 0
    assert "[child] ok" in (tmp_path / "child.log").read_text(encoding="utf-8")


def test_measured_action_records_memory_mean_and_series():
    step = measure_operation("sleepy", lambda: time.sleep(0.3))

    assert step["rss_mean_mb"] > 0
    assert step["peak_mb"] >= step["rss_mean_mb"]
    assert len(step["rss_series_mb"]) >= 1
    assert {"t_s", "mb"} <= set(step["rss_series_mb"][0])
