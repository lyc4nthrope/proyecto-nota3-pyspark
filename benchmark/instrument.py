"""The measurement instrument: wall time and peak process-tree RSS.

Engine agnostic on purpose. The same helpers measure the pandas and the Spark
versions, so both columns of the comparison come from one instrument and the
numbers are comparable.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time

import psutil


def process_tree_rss_mb(root_pid: int | None = None) -> float:
    """Total RSS of a process and all its children (Python + JVM), in MB.

    Defaults to this process; pass a pid to measure another tree (for example
    a child that is about to be killed).
    """
    root = psutil.Process(root_pid if root_pid is not None else os.getpid())
    total = 0
    for process in [root, *root.children(recursive=True)]:
        try:
            total += process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total / (1024**2)


class PeakRssSampler(threading.Thread):
    """Poll process-tree RSS in the background and keep the peak and a series."""

    def __init__(self, interval_s: float = 0.05) -> None:
        super().__init__(daemon=True)
        self.interval_s = interval_s
        self.baseline_mb = process_tree_rss_mb()
        self.peak_mb = self.baseline_mb
        self.samples: list[tuple[float, float]] = []
        self._started_at = time.perf_counter()
        self._stop_event = threading.Event()

    def _sample(self) -> float:
        current = process_tree_rss_mb()
        self.peak_mb = max(self.peak_mb, current)
        self.samples.append(
            (round(time.perf_counter() - self._started_at, 3), round(current, 1))
        )
        return current

    def run(self) -> None:
        while not self._stop_event.wait(self.interval_s):
            self._sample()

    def stop(self) -> float:
        self._stop_event.set()
        self.join()
        self._sample()
        return self.peak_mb

    @property
    def mean_mb(self) -> float:
        if not self.samples:
            return 0.0
        return round(
            sum(value for _, value in self.samples) / len(self.samples), 1
        )

    def series(self, points: int = 60) -> list[dict]:
        """Downsampled memory timeline, for graphing the run."""
        if not self.samples:
            return []
        step = max(1, len(self.samples) // points)
        buckets = [
            self.samples[index : index + step]
            for index in range(0, len(self.samples), step)
        ]
        return [
            {
                "t_s": round(sum(t for t, _ in bucket) / len(bucket), 2),
                "mb": round(sum(v for _, v in bucket) / len(bucket), 1),
            }
            for bucket in buckets
        ]


def measure_operation(operation: str, action) -> dict:
    """Run ``action`` once, capturing elapsed time, peak RSS and any error."""
    sampler = PeakRssSampler()
    started = time.perf_counter()
    sampler.start()
    value = None
    error = None
    try:
        value = action()
    except Exception as exc:  # noqa: BLE001 - the probe records any failure
        error = f"{type(exc).__name__}: {exc}"
    elapsed = time.perf_counter() - started
    peak_mb = sampler.stop()
    return {
        "operation": operation,
        "time_s": round(elapsed, 3),
        "peak_mb": round(peak_mb, 1),
        "baseline_mb": round(sampler.baseline_mb, 1),
        "delta_mb": round(peak_mb - sampler.baseline_mb, 1),
        "rss_mean_mb": sampler.mean_mb,
        "rss_series_mb": sampler.series(),
        "error": error,
        "value": value,
    }


MIN_AVAILABLE_MB = 700
RAM_PERCENT_LIMIT = 95.0


def kill_process_tree(pid: int) -> None:
    """Kill a process and all its children.

    Killing only the direct child is not enough: the measured child spawns a
    JVM, and an orphaned JVM keeps eating the RAM we are trying to free.
    """
    try:
        root = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    for process in [*root.children(recursive=True), root]:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            continue


def run_with_limits(
    command: list[str],
    timeout_s: int,
    log_path=None,
    min_available_mb: int = MIN_AVAILABLE_MB,
    ram_percent_limit: float = RAM_PERCENT_LIMIT,
    poll_s: float = 2.0,
) -> dict:
    """Run ``command`` in its own process tree under time and memory limits.

    The memory limit is the contingency against a run that starts paging: a
    thrashing pandas can freeze Windows long before its timeout fires, so the
    tree is killed as soon as the system crosses the RAM limit and the outcome
    is recorded as such (system RAM >= 95 % is an accepted measured result).

    Output goes to ``log_path`` (a file) instead of a pipe: a pipe buffer fills
    up and blocks the child exactly when it is already struggling.
    """
    log_handle = (
        open(log_path, "w", encoding="utf-8") if log_path else subprocess.DEVNULL
    )
    try:
        process = subprocess.Popen(
            command,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
    finally:
        if log_path:
            log_handle.close()
    kill_reason = None
    tree_mb_at_kill = None
    deadline = time.time() + timeout_s
    while process.poll() is None:
        if time.time() >= deadline:
            kill_reason = f"timeout after {timeout_s}s"
            break
        memory = psutil.virtual_memory()
        if (
            memory.percent >= ram_percent_limit
            or memory.available < min_available_mb * 1024**2
        ):
            kill_reason = (
                f"killed at system RAM {memory.percent:.0f}% "
                f"(available {memory.available / 1024**2:.0f} MB)"
            )
            break
        time.sleep(poll_s)
    if kill_reason:
        tree_mb_at_kill = round(process_tree_rss_mb(process.pid), 1)
        kill_process_tree(process.pid)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
    return {
        "returncode": process.returncode,
        "kill_reason": kill_reason,
        "tree_mb_at_kill": tree_mb_at_kill,
    }


class OperationRecorder:
    """Run a dependency-ordered sequence of measured operations.

    Shared by every engine runner, so the pandas and the Spark measurements
    have the same shape and are comparable row by row. An operation whose
    dependency failed is recorded as skipped and marked failed itself. An
    action may return a plain string or a dict with a ``result`` key plus extra
    fields (for example the in-memory size of the main DataFrame).
    """

    def __init__(self) -> None:
        self.results: list[dict] = []
        self.failed: set[str] = set()

    def run(self, name: str, dependencies: tuple[str, ...], action) -> None:
        blocked = [
            dependency for dependency in dependencies if dependency in self.failed
        ]
        if blocked:
            self.results.append(
                {
                    "operation": name,
                    "result": "-",
                    "time_s": None,
                    "peak_mb": None,
                    "baseline_mb": None,
                    "delta_mb": None,
                    "error": f"skipped: dependency '{blocked[0]}' failed",
                }
            )
            self.failed.add(name)
            return
        measured = measure_operation(name, action)
        value = measured["value"]
        if isinstance(value, dict):
            extra = dict(value)
            result = extra.pop("result", "")
        else:
            extra, result = {}, value
        self.results.append(
            {
                **{key: measured[key] for key in measured if key != "value"},
                "result": result,
                **extra,
            }
        )
        print(
            f"[op] {name}: {measured['time_s']:.3f}s, "
            f"peak {measured['peak_mb']:.1f} MB"
            + (f", ERROR {measured['error']}" if measured["error"] else ""),
            flush=True,
        )
        if measured["error"]:
            self.failed.add(name)
