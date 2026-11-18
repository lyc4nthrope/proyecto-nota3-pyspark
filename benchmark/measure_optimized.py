"""Measure the optimized Spark version end to end (task T4).

Wraps ``spark_version.run`` (Cristhian's optimized pipeline) with the same
instrument as every other measurement, so its seconds and peak memory land in
the evidence with the same balance. The run is recorded as one more run of the
``raw_text`` set with ``engine = spark_optimized``: the comparison report and
the charts pick it up next to ``spark`` and ``pandas``.

    python -m benchmark.measure_optimized --level 1.0 --rep 1
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from benchmark.compare import runs_dir
from benchmark.config import OUTPUT_DIR, RAW_TEXT_DIR
from benchmark.instrument import measure_operation
from benchmark.ladder import CATALOG_PARQUET, level_sample_path, level_tag

DEFAULT_TIMEOUT_LOG = "spark_optimized"


def run_one(fraction: float, rep: int, timeout_s: int) -> dict:
    tag = level_tag(fraction)
    out_dir = OUTPUT_DIR / "spark_optimized" / tag
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "run.log"
    command = [
        sys.executable,
        "-m",
        "spark_version.run",
        "--reviews",
        str(level_sample_path(fraction, RAW_TEXT_DIR)),
        "--catalog",
        str(CATALOG_PARQUET),
        "--output",
        str(out_dir),
        "--explain",
        str(out_dir / "plan.txt"),
    ]

    def action():
        with log_path.open("w", encoding="utf-8") as handle:
            completed = subprocess.run(
                command,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_s,
            )
        if completed.returncode != 0:
            raise RuntimeError(f"spark_version.run exited {completed.returncode}")
        # The optimized CLI prints one JSON line with its own end-to-end time.
        lines = [
            line
            for line in log_path.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("{")
        ]
        return json.loads(lines[-1]) if lines else {}

    measured = measure_operation("spark_optimized", action)
    payload = measured["value"] if not measured["error"] else {}
    seconds = payload.get("seconds", measured["time_s"])
    result = (
        f"{payload.get('cleaned_rows', '?'):,} rows; "
        f"{payload.get('store_rows', '?')} stores; {payload.get('time_rows', '?')} groups"
        if payload
        else "-"
    )
    run = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "label": "raw_text",
        "engine": "spark_optimized",
        "level_pct": round(fraction * 100, 4),
        "level_tag": tag,
        "rep": rep,
        "operations": [
            {
                "operation": "write_both_tables",
                "result": result,
                "time_s": round(seconds, 3),
                "peak_mb": measured["peak_mb"],
                "baseline_mb": measured["baseline_mb"],
                "delta_mb": measured["delta_mb"],
                "rss_mean_mb": measured["rss_mean_mb"],
                "rss_series_mb": measured["rss_series_mb"],
                "wall_s": measured["time_s"],
                "error": measured["error"],
            }
        ],
    }
    out_path = runs_dir("raw_text") / f"spark_optimized_{tag}_r{rep}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(run, indent=2, ensure_ascii=True, default=str), encoding="utf-8"
    )
    print(
        f"[optimized] {tag} rep {rep}: {seconds:.1f}s pipeline, "
        f"{measured['time_s']:.1f}s wall, peak {measured['peak_mb']:.0f} MB",
        flush=True,
    )
    return run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure spark_version.run (optimized) end to end."
    )
    parser.add_argument("--level", type=float, default=1.0)
    parser.add_argument("--rep", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=5400)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    run_one(args.level, args.rep, args.timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
