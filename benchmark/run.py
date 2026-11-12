"""Command line entry point of the benchmark.

Example (3 repetitions of the Spark version on the 10 % sample, 1 hour limit):

    python -m benchmark.run --engine spark --sample pct_010 --repeats 3 --timeout 3600

Each repetition adds one row to output/benchmark/results.csv.
The machine description is saved next to it as environment.json.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from benchmark.measure import (
    DEFAULT_ZONES,
    ENGINES,
    append_result,
    build_command,
    collect_environment,
    measure,
)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the command line options."""
    parser = argparse.ArgumentParser(description="Measure the pandas or the Spark version.")
    parser.add_argument("--engine", required=True, choices=ENGINES)
    parser.add_argument("--sample", required=True, help="Sample folder name, e.g. pct_010")
    parser.add_argument("--repeats", type=int, default=3, help="The assignment requires at least 3")
    parser.add_argument("--timeout", type=float, default=None, help="Seconds before a run is stopped")
    parser.add_argument("--zones", default=DEFAULT_ZONES)
    parser.add_argument("--results", default="output/benchmark/results.csv")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested repetitions and store every result."""
    arguments = parse_arguments(argv)
    results_file = Path(arguments.results)
    results_file.parent.mkdir(parents=True, exist_ok=True)

    environment_file = results_file.with_name("environment.json")
    environment_file.write_text(json.dumps(collect_environment(), indent=2), encoding="utf-8")

    command = build_command(arguments.engine, arguments.sample, zones=arguments.zones)
    for repeat in range(1, arguments.repeats + 1):
        result = measure(command, timeout_seconds=arguments.timeout)
        append_result(results_file, arguments.engine, arguments.sample, repeat, result)
        print(
            f"[{arguments.engine} {arguments.sample} #{repeat}] {result.status}: "
            f"{result.wall_seconds} s, peak {result.peak_rss_mb} MB, cpu {result.cpu_seconds} s"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
