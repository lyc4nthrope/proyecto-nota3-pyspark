"""Build the report charts from the measurement ladder CSV.

Usage (from the repository root):

    python docs/graficos/make_charts.py [path/to/measurements.csv]

Each run in the CSV has one row per operation; a run's total time is the sum of
its operations and its peak memory is the highest peak among them. Every chart
uses the same color per engine (pandas = blue, Spark = orange), checked for
color-blind safety, and every series is also labeled directly so color is
never the only cue. A Markdown table with the same numbers is written next to
the images for readers who need the values.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed: write PNG files only
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).resolve().parent
DEFAULT_CSV = HERE.parent / "evidencia" / "measurements_ladder.csv"

ENGINES = {"pandas": ("pandas", "#2a78d6"), "spark": ("PySpark", "#eb6834")}
TEXT_PRIMARY, TEXT_SECONDARY, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
MACHINE_RAM_GB = 15.3  # test machine (docs/evidencia/report_ladder.md)
OPERATION_NAMES = {
    "read": "Lectura",
    "clean": "Limpieza",
    "derive": "Columnas derivadas",
    "join": "Join con catálogo",
    "aggregate_by_store": "Agregado por tienda",
    "aggregate_by_time": "Agregado temporal",
    "write_parquet": "Escritura Parquet",
}


def load_runs(csv_path: Path) -> pd.DataFrame:
    """Return one row per run (engine, level, repetition) with total time and peak memory."""
    rows = pd.read_csv(csv_path)
    return (
        rows.groupby(["engine", "level_pct", "rep"], as_index=False)
        .agg(total_s=("time_s", "sum"), peak_gb=("peak_mb", "max"))
        .assign(peak_gb=lambda frame: frame["peak_gb"] / 1024)
    )


def summarize(runs: pd.DataFrame) -> pd.DataFrame:
    """Mean, min and max across repetitions for each engine and level."""
    return runs.groupby(["engine", "level_pct"]).agg(
        time_mean=("total_s", "mean"),
        time_min=("total_s", "min"),
        time_max=("total_s", "max"),
        peak_mean=("peak_gb", "mean"),
        peak_max=("peak_gb", "max"),
        runs=("rep", "count"),
    ).reset_index()


def _style(axis, title: str, ylabel: str) -> None:
    """Recessive grid and axes, text in text colors, never in series colors."""
    axis.set_facecolor(SURFACE)
    axis.set_title(title, loc="left", fontsize=12, color=TEXT_PRIMARY, pad=12)
    axis.set_ylabel(ylabel, color=TEXT_SECONDARY)
    axis.grid(axis="y", color=GRID, linewidth=0.8)
    axis.set_axisbelow(True)
    for side in ("top", "right"):
        axis.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        axis.spines[side].set_color(GRID)
    axis.tick_params(colors=TEXT_SECONDARY)


def _level_axis(axis, levels) -> None:
    axis.set_xticks(range(len(levels)), [f"{level:g} %" for level in levels])
    axis.set_xlabel("Muestra de datos (porcentaje de 66,6 M reseñas)", color=TEXT_SECONDARY)


def line_chart(summary: pd.DataFrame, value: str, low: str | None, high: str | None,
               title: str, ylabel: str, unit: str, output: Path, ram_line: bool = False) -> None:
    """One line per engine across the sample levels, with min-max whiskers."""
    figure, axis = plt.subplots(figsize=(8, 4.6), dpi=150, facecolor=SURFACE)
    levels = sorted(summary["level_pct"].unique())
    for engine, (label, color) in ENGINES.items():
        data = summary[summary["engine"] == engine].sort_values("level_pct")
        x = range(len(data))
        axis.plot(x, data[value], color=color, linewidth=2, marker="o", markersize=7,
                  markeredgecolor=SURFACE, markeredgewidth=2, label=label, zorder=3)
        if low and high:
            axis.vlines(x, data[low], data[high], color=color, linewidth=2, alpha=0.45, zorder=2)
        last = data.iloc[-1]
        axis.annotate(f"{label}: {last[value]:,.1f} {unit}".replace(",", "X").replace(".", ",").replace("X", "."),
                      (len(data) - 1, last[value]), xytext=(10, 0), textcoords="offset points",
                      va="center", fontsize=9, color=TEXT_PRIMARY)
    if ram_line:
        axis.axhline(MACHINE_RAM_GB, color=TEXT_SECONDARY, linewidth=1, linestyle="--", zorder=1)
        axis.annotate(f"RAM total del equipo: {MACHINE_RAM_GB:g} GB".replace(".", ","),
                      (0, MACHINE_RAM_GB), xytext=(0, 5), textcoords="offset points",
                      fontsize=9, color=TEXT_SECONDARY)
        axis.set_ylim(0, MACHINE_RAM_GB * 1.12)
    _level_axis(axis, levels)
    _style(axis, title, ylabel)
    axis.set_xlim(-0.3, len(levels) - 1 + 0.9)
    axis.legend(frameon=False, loc="lower right" if ram_line else "upper left", labelcolor=TEXT_PRIMARY)
    figure.tight_layout()
    figure.savefig(output, facecolor=SURFACE)
    plt.close(figure)


def speed_ratio_chart(summary: pd.DataFrame, output: Path) -> None:
    """How many times faster pandas is than Spark at each level (1 = equal)."""
    pivot = summary.pivot(index="level_pct", columns="engine", values="time_mean").sort_index()
    ratio = pivot["spark"] / pivot["pandas"]
    figure, axis = plt.subplots(figsize=(8, 4.2), dpi=150, facecolor=SURFACE)
    x = range(len(ratio))
    axis.plot(x, ratio.values, color=ENGINES["pandas"][1], linewidth=2, marker="o", markersize=7,
              markeredgecolor=SURFACE, markeredgewidth=2, zorder=3)
    for position, value in zip(x, ratio.values):
        axis.annotate(f"{value:.1f}×".replace(".", ","), (position, value), xytext=(0, 9),
                      textcoords="offset points", ha="center", fontsize=9, color=TEXT_PRIMARY)
    axis.axhline(1, color=TEXT_SECONDARY, linewidth=1, linestyle="--")
    axis.annotate("1× = mismo tiempo", (len(ratio) - 1, 1), xytext=(0, -14), textcoords="offset points",
                  ha="right", fontsize=9, color=TEXT_SECONDARY)
    axis.set_ylim(0, ratio.max() * 1.18)
    _level_axis(axis, list(ratio.index))
    _style(axis, "Ventaja de tiempo de pandas sobre PySpark (tiempo Spark ÷ tiempo pandas)",
           "Veces más rápido pandas")
    figure.tight_layout()
    figure.savefig(output, facecolor=SURFACE)
    plt.close(figure)


def operations_chart(csv_path: Path, level: float, output: Path) -> pd.DataFrame:
    """Mean time of each operation at one level, both engines side by side."""
    rows = pd.read_csv(csv_path)
    data = (
        rows[rows["level_pct"] == level]
        .groupby(["operation", "engine"])["time_s"].mean()
        .unstack("engine")
        .reindex(list(OPERATION_NAMES))
    )
    figure, axis = plt.subplots(figsize=(8, 4.8), dpi=150, facecolor=SURFACE)
    height, gap = 0.36, 0.04
    positions = range(len(data))
    for offset, (engine, (label, color)) in zip((-1, 1), ENGINES.items()):
        y = [p + offset * (height + gap) / 2 for p in positions]
        bars = axis.barh(y, data[engine], height=height, color=color, label=label, zorder=3)
        for bar, value in zip(bars, data[engine]):
            axis.annotate(f"{value:.1f} s".replace(".", ","), (bar.get_width(), bar.get_y() + height / 2),
                          xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                          color=TEXT_SECONDARY)
    axis.set_yticks(list(positions), [OPERATION_NAMES[name] for name in data.index])
    axis.invert_yaxis()
    _style(axis, f"Tiempo medio por operación al {level:g} % de los datos", "")
    axis.grid(axis="y", visible=False)
    axis.grid(axis="x", color=GRID, linewidth=0.8)
    axis.set_xlabel("Segundos (media de 3 repeticiones)", color=TEXT_SECONDARY)
    axis.set_xlim(0, data.max().max() * 1.18)
    axis.legend(frameon=False, loc="upper right", labelcolor=TEXT_PRIMARY)
    figure.tight_layout()
    figure.savefig(output, facecolor=SURFACE)
    plt.close(figure)
    return data


def write_table(summary: pd.DataFrame, operations: pd.DataFrame, output: Path) -> None:
    """Write the numbers behind the charts as Markdown (the accessible table view)."""
    lines = [
        "# Datos de los gráficos",
        "",
        "Generado por `docs/graficos/make_charts.py` desde `docs/evidencia/measurements_ladder.csv`.",
        "Tiempo total de una corrida = suma de sus operaciones; pico = el mayor pico entre ellas.",
        "GB con 1 GB = 1024 MB (el reporte de Daniel divide entre 1000: 10 172 MB = 9,93 GB aquí y 10,2 GB allá).",
        "",
        "| Muestra | Motor | Corridas | Tiempo medio (s) | Mín (s) | Máx (s) | Pico medio (GB) | Pico máx (GB) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in summary.sort_values(["level_pct", "engine"]).itertuples():
        lines.append(
            f"| {row.level_pct:g} % | {ENGINES[row.engine][0]} | {row.runs} | {row.time_mean:.1f} | "
            f"{row.time_min:.1f} | {row.time_max:.1f} | {row.peak_mean:.2f} | {row.peak_max:.2f} |"
        )
    lines += ["", "Tiempo medio por operación al 100 % (s):", "",
              "| Operación | pandas | PySpark |", "|---|---|---|"]
    for name, row in operations.iterrows():
        lines.append(f"| {OPERATION_NAMES[name]} | {row['pandas']:.1f} | {row['spark']:.1f} |")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    csv_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_CSV
    summary = summarize(load_runs(csv_path))
    line_chart(summary, "time_mean", "time_min", "time_max",
               "Tiempo total de la canalización según el tamaño de la muestra",
               "Segundos (media de 3 repeticiones)", "s", HERE / "tiempo_total.png")
    line_chart(summary, "peak_mean", None, None,
               "Memoria máxima del proceso según el tamaño de la muestra",
               "GB, 1 GB = 1024 MB (media de 3 repeticiones)", "GB", HERE / "memoria_pico.png",
               ram_line=True)
    speed_ratio_chart(summary, HERE / "ventaja_tiempo.png")
    operations = operations_chart(csv_path, 100.0, HERE / "tiempo_por_operacion_100.png")
    write_table(summary, operations, HERE / "datos_graficos.md")
    print("charts written to", HERE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
