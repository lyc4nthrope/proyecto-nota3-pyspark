"""Naive pandas attempt over the full raw review JSONL (T1, step 5).

The clearest evidence that pandas is not viable: one plain
``pd.read_json(path, lines=True)`` over the 31.4 GB file, measured. It runs as
a subprocess **with a timeout** on purpose: a pandas that starts paging can
keep the machine busy for hours instead of failing cleanly, and the evidence
needs a recorded outcome, not a hung laptop.

The result is written as one more run of the ``raw_text`` run set, so the
comparison report picks it up next to the ladder runs.

Usage:

    python -m benchmark.naive_attempt                 # measure the attempt
    python -m benchmark.naive_attempt --timeout 1200  # seconds
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from benchmark.compare import runs_dir
from benchmark.config import OUTPUT_DIR, REVIEW_JSONL_RAW
from benchmark.instrument import measure_operation, run_with_limits

RUN_NAME = "pandas_naive_100pct_r1.json"
DEFAULT_TIMEOUT = 1200
CHILD_LOG = "naive_attempt_child.log"


def run_child() -> None:
    """The naive attempt itself, in its own process."""
    import pandas as pd

    frame = pd.read_json(REVIEW_JSONL_RAW, lines=True)
    print(f"[child] rows: {len(frame):,}", flush=True)


def measure_attempt(timeout: int) -> dict:
    log_path = OUTPUT_DIR / CHILD_LOG
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def action():
        outcome = run_with_limits(
            [sys.executable, "-m", "benchmark.naive_attempt", "--child"],
            timeout,
            log_path=log_path,
        )
        if outcome["kill_reason"]:
            detail = outcome["kill_reason"]
            if outcome.get("tree_mb_at_kill") is not None:
                detail += (
                    f"; process tree {outcome['tree_mb_at_kill']} MB at kill"
                )
            raise RuntimeError(detail)
        tail = [
            line
            for line in log_path.read_text(encoding="utf-8").splitlines()
            if line.startswith("[")
        ]
        return tail[-1] if tail else f"exit code {outcome['returncode']}"

    measured = measure_operation("pandas_naive_read_json", action)
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "label": "raw_text",
        "engine": "pandas_naive",
        "level_pct": 100.0,
        "level_tag": "100pct",
        "rep": 1,
        "sample_rows": None,
        "operations": [
            {
                **{key: measured[key] for key in measured if key != "value"},
                "result": measured["value"] if not measured["error"] else "-",
            }
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure the naive pandas read of the full raw JSONL."
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    if args.child:
        run_child()
        return 0

    print(
        f"[naive] pd.read_json over {REVIEW_JSONL_RAW.name} "
        f"with a {args.timeout}s timeout",
        flush=True,
    )
    result = measure_attempt(args.timeout)
    out_dir = runs_dir("raw_text")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / RUN_NAME
    out_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
    step = result["operations"][0]
    print(
        f"[naive] time {step['time_s']}s, peak {step['peak_mb']} MB, "
        f"error: {step['error'] or 'none'}",
        flush=True,
    )
    print(f"[naive] saved {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
