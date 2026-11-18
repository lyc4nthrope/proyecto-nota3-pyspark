"""Measure every pandas mitigation technique on every ladder level.

One guarded subprocess per (technique, level), one repetition, exactly as the
handoff asks. Produces the technique-by-level table the report needs:
``docs/evidencia/mitigaciones.md`` and ``.csv``.

    python -m benchmark.run_mitigations                     # everything
    python -m benchmark.run_mitigations --techniques chunks  # one technique
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from benchmark.config import OUTPUT_DIR, RAW_TEXT_DIR, REVIEW_JSONL_RAW
from benchmark.instrument import (
    run_with_limits,
    wait_for_memory_headroom,
)
from benchmark.ladder import level_sample_path, level_tag

CATALOG_PATH = RAW_TEXT_DIR.parent / "ladder" / "catalog.parquet"
MITIGATIONS_DIR = OUTPUT_DIR / "mitigations"
EVIDENCE_DIR = Path(__file__).resolve().parents[1] / "docs" / "evidencia"
DEFAULT_TECHNIQUES = "columns,dtypes,chunks"
DEFAULT_LEVELS = "0.01,0.1,0.5,1.0"
DEFAULT_TIMEOUT = 3600
COLUMNS = [
    "technique", "level", "rows", "status", "seconds",
    "peak_tree_mb", "frames_mb", "notes",
]


def level_rows(fraction: float) -> int:
    manifest_path = level_sample_path(fraction, RAW_TEXT_DIR).with_suffix(
        ".manifest.json"
    )
    return int(json.loads(manifest_path.read_text(encoding="utf-8"))["sample_rows"])


def run_one(technique: str, fraction: float, timeout: int) -> dict:
    tag = level_tag(fraction)
    out_dir = MITIGATIONS_DIR / f"{technique}_{tag}"
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "run.log"
    if technique == "chunks":
        command = [
            sys.executable, "-m", "pandas_version.mitigated",
            "--technique", "chunks",
            "--interactions", str(REVIEW_JSONL_RAW),
            "--catalog", str(CATALOG_PATH),
            "--output", str(out_dir),
            "--max-rows", str(level_rows(fraction)),
        ]
    else:
        command = [
            sys.executable, "-m", "pandas_version.mitigated",
            "--technique", technique,
            "--interactions", str(level_sample_path(fraction, RAW_TEXT_DIR)),
            "--catalog", str(CATALOG_PATH),
            "--output", str(out_dir),
        ]
    waited = wait_for_memory_headroom()
    if waited > 1:
        print(f"[guard] waited {waited}s for memory headroom", flush=True)
    outcome = run_with_limits(command, timeout, log_path=log_path)
    summary_path = out_dir / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        status = "termino"
        frames = summary.get("frames_mb", {})
    else:
        frames = {}
        status = outcome["kill_reason"] or f"exit code {outcome['returncode']}"
        summary = {"rows": level_rows(fraction), "seconds": None, "notes": status}
    return {
        "technique": technique,
        "level": tag,
        "rows": summary.get("rows"),
        "status": status,
        "seconds": summary.get("seconds"),
        "peak_tree_mb": outcome.get("tree_private_mb_at_kill")
        if outcome.get("kill_reason")
        else None,
        "frames_mb": frames,
        "notes": summary.get("notes", ""),
    }


def render(records: list[dict]) -> str:
    lines = [
        "# Mitigaciones de pandas sobre la ruta con texto",
        "",
        f"Generado: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        "Una repetición por técnica y nivel, con el mismo instrumento que la "
        "escalera (proceso propio, guardia de RAM, enfriamiento entre corridas).",
        "",
        "| Técnica | Nivel | Filas | Estado | Tiempo (s) | Memoria | Notas |",
        "|---|---|---|---|---|---|---|",
    ]
    for record in records:
        frames = record.get("frames_mb") or {}
        memory = frames.get("joined") or record.get("peak_tree_mb") or "-"
        lines.append(
            f"| {record['technique']} | {record['level']} | "
            f"{record['rows']:,} | {record['status']} | "
            f"{record['seconds'] if record['seconds'] is not None else '-'} | "
            f"{memory} | {record['notes'] or ''} |"
        )
    lines.extend(["", "Ver tabla por operación en el reporte de la escalera.", ""])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure pandas mitigation techniques per level."
    )
    parser.add_argument("--techniques", default=DEFAULT_TECHNIQUES)
    parser.add_argument("--levels", default=DEFAULT_LEVELS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    techniques = [item.strip() for item in args.techniques.split(",") if item.strip()]
    fractions = [float(item) for item in args.levels.split(",") if item.strip()]

    records = []
    for fraction in fractions:
        for technique in techniques:
            print(
                f"[mitigated] {technique} level {level_tag(fraction)}",
                flush=True,
            )
            records.append(run_one(technique, fraction, args.timeout))

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / "mitigaciones.md").write_text(render(records), encoding="utf-8")
    with (EVIDENCE_DIR / "mitigaciones.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    **record,
                    "frames_mb": json.dumps(record.get("frames_mb") or {}),
                }
            )
    print(f"Saved: {EVIDENCE_DIR / 'mitigaciones.md'}", flush=True)
    print(f"Saved: {EVIDENCE_DIR / 'mitigaciones.csv'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
