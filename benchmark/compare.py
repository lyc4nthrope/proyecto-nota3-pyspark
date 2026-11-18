"""pandas vs PySpark comparison over the deterministic 1/10/50/100% ladder.

Modes:

- ``python -m benchmark.compare`` -- build the samples, run every
  (engine, level, repetition) and write the comparison report.
- ``python -m benchmark.compare --build-only`` -- prepare the shared catalog
  Parquet and the level samples, then exit.
- ``python -m benchmark.compare --single-run --engine pandas --level 0.1
  --rep 1`` -- one measured run; used by the orchestrator.

Why one subprocess per (engine, level, repetition): a fresh process starts with
clean memory. pandas' allocator and the JVM both keep freed pages resident, so
repetitions inside one process would each look worse than the previous one and
the peak RSS of repetition 3 would measure the previous runs, not this one.

The ladder itself (``benchmark.ladder``) grows the fact rows with a
deterministic row hash and joins every level against the same full catalog, so
the analysis stays constant while the data grows.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psutil

import pandas_version.analysis  # noqa: F401 - fails fast without pandas
from benchmark import ladder, report
from benchmark.config import DATASET_ID, OUTPUT_DIR
from benchmark.pandas_runner import pandas_environment_info, run_pandas_analysis
from benchmark.runner import environment_info, run_analysis
from spark_version.session import build_spark_session

RUNS_DIR = OUTPUT_DIR / "runs"
DEFAULT_LEVELS = "0.01,0.1,0.5,1.0"
DEFAULT_ENGINES = "pandas,spark"
DEFAULT_REPEAT = 3
DEFAULT_TIMEOUT = 3600


def orchestrator_environment_info() -> dict:
    memory = psutil.virtual_memory()
    return {
        "dataset": DATASET_ID,
        "python": platform.python_version(),
        "os": platform.platform(),
        "cpu_logical": psutil.cpu_count(logical=True),
        "ram_total_gb": round(memory.total / (1024**3), 1),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare pandas and PySpark on deterministic Home_and_Kitchen "
            "row-fraction samples (1/10/50/100%)."
        )
    )
    parser.add_argument("--levels", default=DEFAULT_LEVELS)
    parser.add_argument("--engines", default=DEFAULT_ENGINES)
    parser.add_argument("--repeat", type=int, default=DEFAULT_REPEAT)
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--single-run", action="store_true")
    parser.add_argument("--engine", choices=["pandas", "spark"])
    parser.add_argument("--level", type=float)
    parser.add_argument("--rep", type=int, default=1)
    return parser.parse_args(argv)


def _levels(text: str) -> list[float]:
    values = [float(item) for item in text.split(",") if item.strip()]
    for value in values:
        if not 0 < value <= 1:
            raise ValueError(f"level {value} must be in (0, 1]")
    return values


def build_samples(levels: list[float]) -> list[dict]:
    """Prepare the shared catalog and one sample per level (needs Spark)."""
    spark = build_spark_session(
        app_name="ladder-build", driver_memory="4g"
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ladder.ensure_full_catalog(spark)
        return [ladder.ensure_level_sample(spark, level) for level in levels]
    finally:
        spark.stop()


def _level_manifest(fraction: float) -> dict:
    manifest_path = ladder.level_sample_path(fraction).with_suffix(".manifest.json")
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"{manifest_path.name} is missing; run --build-only first"
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def single_run(args: argparse.Namespace) -> int:
    fraction = args.level
    tag = ladder.level_tag(fraction)
    manifest = _level_manifest(fraction)
    sample_path = Path(manifest["sample_path"])
    catalog_path = ladder.CATALOG_PARQUET
    sample_rows = int(manifest["sample_rows"])

    if args.engine == "spark":
        spark = build_spark_session(
            app_name=f"ladder-spark-{tag}-r{args.rep}",
            driver_memory=args.driver_memory,
        )
        spark.sparkContext.setLogLevel("ERROR")
        try:
            environment = environment_info(spark)
            operations = run_analysis(
                spark, sample_path, catalog_path, sample_rows
            )
        finally:
            spark.stop()
    else:
        environment = pandas_environment_info()
        operations = run_pandas_analysis(
            sample_path, catalog_path, sample_rows
        )

    result = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine": args.engine,
        "level_pct": round(fraction * 100, 4),
        "level_tag": tag,
        "rep": args.rep,
        "sample_rows": sample_rows,
        "sample_path": str(sample_path),
        "catalog_path": str(catalog_path),
        "environment": environment,
        "operations": operations,
    }
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RUNS_DIR / f"{args.engine}_{tag}_r{args.rep}.json"
    out_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
    failed = [step for step in operations if step.get("error")]
    print(
        f"[run] {args.engine} {tag} rep {args.rep}: "
        f"{'FAILED ' + str(len(failed)) if failed else 'ok'} -> {out_path.name}",
        flush=True,
    )
    return 1 if failed else 0


def _spawn_run(args: argparse.Namespace, engine: str, fraction: float, rep: int) -> dict:
    command = [
        sys.executable,
        "-m",
        "benchmark.compare",
        "--single-run",
        "--engine",
        engine,
        "--level",
        str(fraction),
        "--rep",
        str(rep),
        "--driver-memory",
        args.driver_memory,
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )
    except subprocess.TimeoutExpired:
        return {
            "engine": engine,
            "level_pct": round(fraction * 100, 4),
            "level_tag": ladder.level_tag(fraction),
            "rep": rep,
            "operations": [
                {
                    "operation": "run",
                    "result": "-",
                    "time_s": None,
                    "peak_mb": None,
                    "baseline_mb": None,
                    "delta_mb": None,
                    "error": f"timeout after {args.timeout}s",
                }
            ],
        }
    tail = [line for line in completed.stdout.splitlines() if line.startswith("[")]
    for line in tail[-3:]:
        print(f"  {line}", flush=True)
    run_path = (
        RUNS_DIR / f"{engine}_{ladder.level_tag(fraction)}_r{rep}.json"
    )
    if not run_path.exists():
        return {
            "engine": engine,
            "level_pct": round(fraction * 100, 4),
            "level_tag": ladder.level_tag(fraction),
            "rep": rep,
            "operations": [
                {
                    "operation": "run",
                    "result": "-",
                    "time_s": None,
                    "peak_mb": None,
                    "baseline_mb": None,
                    "delta_mb": None,
                    "error": (completed.stderr or completed.stdout)[-500:],
                }
            ],
        }
    return json.loads(run_path.read_text(encoding="utf-8"))


def orchestrate(args: argparse.Namespace) -> int:
    levels = _levels(args.levels)
    engines = [item.strip() for item in args.engines.split(",") if item.strip()]
    for engine in engines:
        if engine not in ("pandas", "spark"):
            raise ValueError(f"unknown engine {engine!r}")
    if args.repeat < 1:
        raise ValueError("--repeat must be >= 1")

    if not args.skip_build:
        print(f"[build] preparing catalog and levels {args.levels}", flush=True)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "benchmark.compare",
                "--build-only",
                "--levels",
                args.levels,
                "--driver-memory",
                args.driver_memory,
            ],
            timeout=max(args.timeout, 7200),
        )
        if completed.returncode != 0:
            print("[fatal] sample build failed")
            return 1

    runs: list[dict] = []
    for fraction in levels:
        for engine in engines:
            for rep in range(1, args.repeat + 1):
                print(
                    f"[run] {engine} level {ladder.level_tag(fraction)} "
                    f"rep {rep}/{args.repeat}",
                    flush=True,
                )
                runs.append(_spawn_run(args, engine, fraction, rep))

    comparison = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "argv": [Path(sys.executable).name, *sys.argv],
        "environment": orchestrator_environment_info(),
        "repeat": args.repeat,
        "engines": engines,
        "levels": [
            {
                "fraction": fraction,
                "level_tag": ladder.level_tag(fraction),
                **_level_manifest(fraction),
            }
            for fraction in levels
        ],
        "runs": runs,
    }
    report.save_comparison(comparison)
    print()
    print(report.render_comparison(comparison))
    print()
    print(f"Saved: {OUTPUT_DIR / 'report_ladder.md'}")
    print(f"Saved: {OUTPUT_DIR / 'measurements_ladder.csv'}")
    print(f"Saved: {OUTPUT_DIR / 'run_ladder.json'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    if args.build_only:
        manifests = build_samples(_levels(args.levels))
        print(f"[build] {len(manifests)} level samples ready")
        return 0
    if args.single_run:
        if args.engine is None or args.level is None:
            print("[fatal] --single-run needs --engine and --level")
            return 2
        return single_run(args)
    return orchestrate(args)


if __name__ == "__main__":
    raise SystemExit(main())
