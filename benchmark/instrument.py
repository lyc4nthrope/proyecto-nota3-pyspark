"""The measurement instrument: wall time and peak process-tree RSS.

Engine agnostic on purpose. The same helpers measure the pandas and the Spark
versions, so both columns of the comparison come from one instrument and the
numbers are comparable.
"""

from __future__ import annotations

import os
import threading
import time

import psutil


def process_tree_rss_mb() -> float:
    """Total RSS of this process and all its children (Python + JVM), in MB."""
    root = psutil.Process(os.getpid())
    total = 0
    for process in [root, *root.children(recursive=True)]:
        try:
            total += process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total / (1024**2)


class PeakRssSampler(threading.Thread):
    """Poll process-tree RSS in the background and keep the peak."""

    def __init__(self, interval_s: float = 0.05) -> None:
        super().__init__(daemon=True)
        self.interval_s = interval_s
        self.baseline_mb = process_tree_rss_mb()
        self.peak_mb = self.baseline_mb
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(self.interval_s):
            self.peak_mb = max(self.peak_mb, process_tree_rss_mb())

    def stop(self) -> float:
        self._stop_event.set()
        self.join()
        self.peak_mb = max(self.peak_mb, process_tree_rss_mb())
        return self.peak_mb


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
        "error": error,
        "value": value,
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
