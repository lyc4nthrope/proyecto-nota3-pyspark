"""Measure time, memory and CPU of one run of the pandas or the Spark version.

The same code measures both engines, as an external observer of the process.
That keeps the comparison fair: neither version measures itself.

Works on Linux and Windows (everything goes through `psutil`).
"""

from __future__ import annotations

import csv
import platform
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

import psutil

MB = 1024 * 1024
GB = 1024 * MB

ENGINES = ("pandas", "spark")
DEFAULT_ZONES = "data/raw/taxi_zone_lookup.csv"

RESULT_COLUMNS = [
    "timestamp_utc",
    "engine",
    "sample",
    "repeat",
    "status",
    "exit_code",
    "wall_seconds",
    "peak_rss_mb",
    "cpu_seconds",
    "peak_system_memory_percent",
]


@dataclass(frozen=True)
class Measurement:
    """Outcome of one measured run.

    Attributes:
        status: "ok" (exit code 0), "failed" (any other exit code, e.g. a
            MemoryError or the process being killed) or "timeout".
        exit_code: Exit code of the process; None when it was stopped by timeout.
        wall_seconds: Real elapsed time, start to end.
        peak_rss_mb: Highest RAM in use by the process and all its children.
        cpu_seconds: CPU time (user + system) used by the process tree.
        peak_system_memory_percent: Highest RAM usage of the whole machine.
    """

    status: str
    exit_code: int | None
    wall_seconds: float
    peak_rss_mb: float
    cpu_seconds: float
    peak_system_memory_percent: float


def build_command(
    engine: str,
    sample: str,
    zones: str = DEFAULT_ZONES,
    samples_dir: str = "data/samples",
    output_dir: str = "output",
) -> list[str]:
    """Build the command line defined in docs/CONTRATO_ANALISIS.md, section 8.

    Example:
        >>> build_command("pandas", "pct_001")[1:4]
        ['-m', 'pandas_version.run', '--input']
    """
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {ENGINES}, got {engine!r}")
    return [
        sys.executable,
        "-m",
        f"{engine}_version.run",
        "--input",
        f"{samples_dir}/{sample}",
        "--zones",
        zones,
        "--output",
        f"{output_dir}/{engine}/{sample}",
    ]


def _process_tree(root: psutil.Process) -> list[psutil.Process]:
    """Return the process and every descendant that is still alive."""
    try:
        return [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return []


def _stop_process_tree(root: psutil.Process) -> None:
    """Kill the process and all its children (for example Spark's JVM)."""
    processes = _process_tree(root)
    for process in processes:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(processes, timeout=10)


def measure(
    command: list[str],
    timeout_seconds: float | None = None,
    poll_seconds: float = 0.1,
    cwd: str | Path | None = None,
) -> Measurement:
    """Run a command and record its time, peak memory and CPU time.

    Memory is sampled every `poll_seconds` over the whole process tree,
    because Spark's memory lives in a JVM child process, not in Python.
    A spike shorter than the polling interval can be missed.

    Args:
        command: Program and arguments, as a list (never a shell string).
        timeout_seconds: Stop the run after this many seconds; None = no limit.
        poll_seconds: Time between two samples.
        cwd: Working directory for the command.

    Returns:
        A Measurement. The function never raises because the command failed:
        a failure is a result ("failed" or "timeout"), not an error.
    """
    peak_rss = 0
    peak_system_percent = 0.0
    cpu_by_pid: dict[int, float] = {}

    started = time.perf_counter()
    root = psutil.Popen(command, cwd=cwd)
    status, exit_code = "ok", None

    while True:
        # Sample first, so even very short runs get at least one reading.
        rss_now = 0
        for process in _process_tree(root):
            try:
                rss_now += process.memory_info().rss
                times = process.cpu_times()
                # Keep the last value seen per process: it survives the process exit.
                cpu_by_pid[process.pid] = times.user + times.system
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        peak_rss = max(peak_rss, rss_now)
        peak_system_percent = max(peak_system_percent, psutil.virtual_memory().percent)

        try:
            exit_code = root.wait(timeout=poll_seconds)
            status = "ok" if exit_code == 0 else "failed"
            break
        except psutil.TimeoutExpired:
            pass

        if timeout_seconds is not None and time.perf_counter() - started > timeout_seconds:
            _stop_process_tree(root)
            status, exit_code = "timeout", None
            break

    return Measurement(
        status=status,
        exit_code=exit_code,
        wall_seconds=round(time.perf_counter() - started, 3),
        peak_rss_mb=round(peak_rss / MB, 1),
        cpu_seconds=round(sum(cpu_by_pid.values()), 2),
        peak_system_memory_percent=peak_system_percent,
    )


def append_result(
    results_file: str | Path,
    engine: str,
    sample: str,
    repeat: int,
    result: Measurement,
) -> None:
    """Append one run to the results CSV, writing the header only once."""
    path = Path(results_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_COLUMNS)
        if is_new_file:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "engine": engine,
                "sample": sample,
                "repeat": repeat,
                "status": result.status,
                "exit_code": "" if result.exit_code is None else result.exit_code,
                "wall_seconds": result.wall_seconds,
                "peak_rss_mb": result.peak_rss_mb,
                "cpu_seconds": result.cpu_seconds,
                "peak_system_memory_percent": result.peak_system_memory_percent,
            }
        )


def _installed_version(package: str) -> str:
    """Return the installed version of a package, or 'not installed'."""
    try:
        return metadata.version(package)
    except metadata.PackageNotFoundError:
        return "not installed"


def collect_environment() -> dict:
    """Describe the test machine: the assignment requires CPU, RAM, disk and versions."""
    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu_model": platform.processor() or platform.machine(),
        "cpu_physical_cores": psutil.cpu_count(logical=False),
        "cpu_logical_cores": psutil.cpu_count(logical=True),
        "ram_total_gb": round(psutil.virtual_memory().total / GB, 1),
        "disk_free_gb": round(shutil.disk_usage(Path.cwd()).free / GB, 1),
        "python_version": platform.python_version(),
        "pandas_version": _installed_version("pandas"),
        "pyspark_version": _installed_version("pyspark"),
        "pyarrow_version": _installed_version("pyarrow"),
    }
