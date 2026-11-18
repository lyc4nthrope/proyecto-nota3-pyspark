"""Rendering and persistence of the measurement report.

One report per run: a Markdown document for humans, a CSV of the measured
operations for charts, and the full JSON for later analysis.
"""

from __future__ import annotations

import csv
import json

from benchmark.config import OUTPUT_DIR


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def render_report(report: dict) -> str:
    environment = report["environment"]
    source = report["source"]
    sample = report["sample"]
    profile = report["profile"]
    lines = [
        "# Baseline measurement on an Amazon Home_and_Kitchen 1% sample",
        "",
        f"Generated: {report['created_utc']}",
        "",
        "## Environment",
        "",
        _markdown_table(
            ["Item", "Value"],
            [[key, value] for key, value in environment.items()],
        ),
        "",
        "## Source and sample",
        "",
        _markdown_table(
            ["Item", "Value"],
            [
                ["Dataset", source["dataset"]],
                ["Review table", source["review_table"]],
                ["Review table size", f"{source['review_mb']:.1f} MB"],
                ["Full review rows", f"{sample['review_rows']:,}"],
                ["Sample rows", f"{sample['sample_rows']:,}"],
                ["Actual review fraction", f"{sample['actual_fraction'] * 100:.4f}%"],
                ["Metadata source", source["meta_table"]],
                ["Metadata size", f"{source['meta_mb']:.1f} MB"],
                ["Metadata sample bytes", f"{source['meta_sample_mb']:.1f} MB"],
                ["Metadata windows", f"{source['meta_windows']:,}"],
                ["Sampled products", f"{sample['meta_products']:,}"],
                ["Sample path", sample["sample_path"]],
            ],
        ),
        "",
        "The product sample is 1% of metadata bytes in deterministic, "
        "line-aligned windows. Every 0core interaction for those products is "
        "kept, so the join has full catalog coverage. The actual review "
        "fraction is measured, not forced: the product cluster can differ "
        "slightly from 1% of rows.",
        "",
        "The raw review JSONL (31.4 GB, with text) is not downloaded for this "
        "probe; the published 0core interaction table (3.86 GB) is the source "
        "population. The final pipeline can use the raw JSONL for text.",
        "",
        "## Sample profile",
        "",
        _markdown_table(
            ["Item", "Value"],
            [
                ["Rows", f"{profile['rows']:,}"],
                ["Columns", profile["columns"]],
                ["On-disk size (MB)", profile["disk_mb"]],
                ["Distinct users", f"{profile['distinct_users']:,}"],
                ["Distinct products", f"{profile['distinct_products']:,}"],
                ["Reviews per product (avg)", profile["reviews_per_product"]],
                ["Timestamp range",
                 f"{profile['timestamp_min']} .. {profile['timestamp_max']}"],
            ],
        ),
        "",
        _markdown_table(
            ["Rating", "Reviews"],
            [
                [item["rating"], f"{item['reviews']:,}"]
                for item in profile["rating_counts"]
            ],
        ),
        "",
        _markdown_table(
            ["Column", "Type", "Nulls", "Null %", "Min", "Max", "Mean"],
            [
                [
                    item["name"],
                    item["type"],
                    f"{item['nulls']:,}",
                    f"{item['null_pct']:.2f}",
                    item["min"],
                    item["max"],
                    item["mean"],
                ]
                for item in profile["column_profile"]
            ],
        ),
        "",
        "## Preparation measurements",
        "",
        _markdown_table(
            ["Operation", "Result", "Time (s)", "Peak RSS (MB)", "Delta (MB)", "Error"],
            [
                [
                    step["operation"],
                    step["result"],
                    f"{step['time_s']:.3f}",
                    f"{step['peak_mb']:.1f}",
                    f"{step['delta_mb']:.1f}",
                    step["error"] or "",
                ]
                for step in report["preparation"]
            ],
        ),
        "",
        "## Analysis operations, one by one",
        "",
        _markdown_table(
            [
                "Operation",
                "Result",
                "Time (s)",
                "Peak RSS (MB)",
                "Delta (MB)",
                "Error",
            ],
            [
                [
                    step["operation"],
                    step["result"],
                    "-" if step["time_s"] is None else f"{step['time_s']:.3f}",
                    "-" if step["peak_mb"] is None else f"{step['peak_mb']:.1f}",
                    "-" if step["delta_mb"] is None else f"{step['delta_mb']:.1f}",
                    step["error"] or "",
                ]
                for step in report["operations"]
            ],
        ),
        "",
        "## Notes",
        "",
        "- Times are cumulative wall time for each action, because Spark is "
        "lazy and nothing is cached: operation N re-executes 1..N.",
        "- Peak RSS is the total resident memory of the Python process plus its "
        "JVM children, sampled every 50 ms with psutil. It is not JVM heap and "
        "does not include OS page cache.",
        "- RSS is not monotonic: the JVM can return pages to the OS between "
        "actions, so a later peak can be lower than an earlier one. Compare the "
        "Delta (peak minus that operation's baseline), not raw peaks alone.",
        "- This probe uses no optimization: no cache, no broadcast hint, no "
        "repartition, no tuned shuffle, and no tuned Spark configuration.",
        "- A failure is recorded in the Error column and its dependents are "
        "marked as skipped; the run does not abort.",
        "",
        "## Cleaning rules used by this probe",
        "",
        "- Drop rows with nulls in: user_id, parent_asin, rating, timestamp.",
        "- Keep only 1 <= rating <= 5 and timestamp > 0.",
        "- Types are encoded in the sample Parquet; the probe does not re-cast "
        "them.",
        "",
    ]
    return "\n".join(lines)


def save_report(report: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "run.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
    with (OUTPUT_DIR / "measurements.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "operation",
                "result",
                "time_s",
                "peak_mb",
                "baseline_mb",
                "delta_mb",
                "rss_mean_mb",
                "error",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(report["operations"])
    (OUTPUT_DIR / "report.md").write_text(
        render_report(report), encoding="utf-8"
    )


def _run_total_time(operations: list[dict]) -> float:
    return round(
        sum(step["time_s"] or 0.0 for step in operations), 3
    )


def _run_one_pass_time(operations: list[dict]):
    """Time of the write_parquet action: the cost of producing both output
    tables from scratch.

    Spark is lazy and nothing is cached, so the sum of the per-operation times
    re-counts the same work several times. The eager pandas sum is a single
    pass; comparing it against Spark's write_parquet action compares the same
    thing: one complete production of both tables.
    """
    for step in operations:
        if step["operation"] == "write_parquet":
            return step["time_s"]
    return None


def _run_peak_mb(operations: list[dict]) -> float:
    peaks = [step["peak_mb"] for step in operations if step.get("peak_mb")]
    return round(max(peaks), 1) if peaks else 0.0


def render_comparison(comparison: dict) -> str:
    """Markdown report of the pandas vs PySpark ladder comparison."""
    levels = comparison["levels"]
    runs = comparison["runs"]
    lines = [
        "# pandas vs PySpark - Home_and_Kitchen ladder (1/10/50/100%)",
        "",
        f"Generated: {comparison['created_utc']}",
        "",
        "## Environment",
        "",
        _markdown_table(
            ["Item", "Value"],
            [[key, value] for key, value in comparison["environment"].items()],
        ),
        "",
        "## Ladder levels",
        "",
        _markdown_table(
            ["Level", "Rows", "Actual fraction", "Sample (MB)", "Seconds"],
            [
                [
                    level["level_tag"],
                    f"{level['sample_rows']:,}",
                    f"{level['actual_fraction'] * 100:.4f}%",
                    round(level["sample_bytes"] / (1024**2), 1),
                    level["seconds"],
                ]
                for level in levels
            ],
        ),
        "",
        "Fact rows are a deterministic hash fraction of the published 0core "
        "interaction table. Every level joins against the same full product "
        "catalog (shared Parquet), so the analysis is constant and only the "
        "data grows.",
        "",
    ]

    for level in levels:
        level_runs = [
            run for run in runs if run["level_tag"] == level["level_tag"]
        ]
        lines.extend(
            [
                f"## Level {level['level_tag']} "
                f"({level['sample_rows']:,} rows)",
                "",
                _markdown_table(
                    [
                        "Operation",
                        "Engine",
                        "Rep",
                        "Result",
                        "Time (s)",
                        "Peak RSS (MB)",
                        "Delta (MB)",
                        "Frame (MB)",
                        "Error",
                    ],
                    [
                        [
                            step["operation"],
                            run["engine"],
                            run["rep"],
                            step["result"],
                            "-" if step["time_s"] is None else f"{step['time_s']:.3f}",
                            "-"
                            if step["peak_mb"] is None
                            else f"{step['peak_mb']:.1f}",
                            "-"
                            if step["delta_mb"] is None
                            else f"{step['delta_mb']:.1f}",
                            step.get("frame_mb", "-") or "-",
                            step["error"] or "",
                        ]
                        for run in level_runs
                        for step in run["operations"]
                    ],
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## Summary (per engine and level)",
            "",
            _markdown_table(
                [
                    "Level",
                    "Engine",
                    "Runs",
                    "Sum of ops mean (s)",
                    "Sum of ops std (s)",
                    "One-pass mean (s)",
                    "Max peak RSS (MB)",
                    "Status",
                ],
                [
                    [
                        level["level_tag"],
                        engine,
                        len(group),
                        round(
                            sum(_run_total_time(run["operations"]) for run in group)
                            / len(group),
                            2,
                        ),
                        round(
                            _std(
                                [_run_total_time(run["operations"]) for run in group]
                            ),
                            2,
                        ),
                        (
                            round(
                                sum(
                                    _run_one_pass_time(run["operations"])
                                    for run in group
                                )
                                / len(group),
                                2,
                            )
                            if any(
                                _run_one_pass_time(run["operations"]) is not None
                                for run in group
                            )
                            else "-"
                        ),
                        max(_run_peak_mb(run["operations"]) for run in group),
                        "ok"
                        if all(
                            not step.get("error")
                            for run in group
                            for step in run["operations"]
                        )
                        else "failed ops recorded",
                    ]
                    for level in levels
                    for engine in comparison["engines"]
                    for group in [
                        [
                            run
                            for run in runs
                            if run["level_tag"] == level["level_tag"]
                            and run["engine"] == engine
                        ]
                    ]
                    if group
                ],
            ),
            "",
            "## Notes",
            "",
            "- Each (engine, level, repetition) runs in a fresh process, so the "
            "peak RSS belongs to that run alone and not to allocator or JVM "
            "memory kept from previous ones.",
            "- Times are cumulative wall time per action, because nothing is "
            "cached: operation N re-executes operations 1..N.",
            "- Because of that, the sum of the operation times over-counts "
            "Spark's work (the same read runs once per action). The one-pass "
            "column is the write_parquet action: the cost of producing both "
            "output tables from scratch. pandas is eager, so its sum already "
            "is one pass; compare those two columns, not the sums.",
            "- For the failed pandas runs at 50/100 % there is no one-pass "
            "time: the run is a kill record, not a timing.",
            "- Peak RSS is total resident memory of the Python process plus its "
            "JVM children, sampled every 50 ms; it is not JVM heap.",
            "- Frame (MB) is the deep in-memory size of the pandas DataFrame "
            "that the operation carries; Spark has no equivalent column.",
            "- Averages and standard deviations are rounded to 4 decimals; the "
            "two engines use different rounding modes, so an exact tie may "
            "differ in the last decimal.",
            "- No optimization is applied here: no cache, no broadcast hint, no "
            "repartition and no tuned shuffle.",
            "- A failure is recorded in the Error column; its dependents are "
            "skipped and the run does not abort. A run that exceeds the timeout "
            "is recorded as such.",
            "",
        ]
    )
    return "\n".join(lines)


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return (sum((value - mean) ** 2 for value in values) / (len(values) - 1)) ** 0.5


def save_comparison(comparison: dict, label: str = "ladder") -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / f"run_{label}.json").write_text(
        json.dumps(comparison, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
    with (OUTPUT_DIR / f"measurements_{label}.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "level_pct",
                "engine",
                "rep",
                "operation",
                "result",
                "time_s",
                "peak_mb",
                "baseline_mb",
                "delta_mb",
                "rss_mean_mb",
                "frame_mb",
                "error",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        for run in comparison["runs"]:
            for step in run["operations"]:
                writer.writerow(
                    {
                        "level_pct": run["level_pct"],
                        "engine": run["engine"],
                        "rep": run["rep"],
                        "operation": step["operation"],
                        "result": step["result"],
                        "time_s": step["time_s"],
                        "peak_mb": step["peak_mb"],
                        "baseline_mb": step["baseline_mb"],
                        "delta_mb": step["delta_mb"],
                        "rss_mean_mb": step.get("rss_mean_mb", ""),
                        "frame_mb": step.get("frame_mb", ""),
                        "error": step["error"] or "",
                    }
                )
    (OUTPUT_DIR / f"report_{label}.md").write_text(
        render_comparison(comparison), encoding="utf-8"
    )
