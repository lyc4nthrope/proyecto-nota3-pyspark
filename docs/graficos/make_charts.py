"""Build the report charts from a measurement CSV.

Usage (from the repository root):

    python docs/graficos/make_charts.py [path/to/measurements.csv]

The default input is the raw-text route (``measurements_raw_text.csv``), where
pandas cannot finish the larger samples.

Each run in the CSV has one row per operation; a run's total time is the sum of
its operations and its peak memory is the highest peak among them. A run with
an ``error`` (killed by the RAM guard) has no valid time: it is left out of
every average and drawn instead as an "X" at the private memory it had
committed when it was killed, so a failure never looks like a fast run.

Every chart uses the same color per engine (pandas = blue, Spark = orange),
checked for color-blind safety, and every series is also labeled directly so
color is never the only cue. A Markdown table with the same numbers is written
next to the images for readers who need the values.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed: write PNG files only
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).resolve().parent
DEFAULT_CSV = HERE.parent / "evidencia" / "measurements_raw_text.csv"

ENGINES = {"pandas": ("pandas", "#2a78d6"), "spark": ("PySpark", "#eb6834")}
TEXT_PRIMARY, TEXT_SECONDARY, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
MACHINE_RAM_GB = 15.3  # test machine (docs/evidencia/report_raw_text.md)
MB_PER_GB = 1024
OPERATIONS_LEVEL = 10.0  # largest sample both engines finish
OPERATION_NAMES = {
    "read": "Lectura",
    "clean": "Limpieza",
    "derive": "Columnas derivadas",
    "join": "Join con catálogo",
    "aggregate_by_store": "Agregado por tienda",
    "aggregate_by_time": "Agregado temporal",
    "write_parquet": "Escritura Parquet",
}
# "... process tree 13894.2 MB private (8344.4 MB resident) at kill"
PRIVATE_AT_KILL = re.compile(r"([\d.]+) MB private")


def _es(value: float, decimals: int = 1) -> str:
    """Format a number the Spanish way: 1.234,5."""
    text = f"{value:,.{decimals}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def load_rows(csv_path: Path) -> pd.DataFrame:
    """Read the CSV, keeping only the two engines compared in the charts."""
    rows = pd.read_csv(csv_path)
    return rows[rows["engine"].isin(ENGINES)]


def load_runs(rows: pd.DataFrame) -> pd.DataFrame:
    """One row per finished run (engine, level, repetition): total time and peak memory.

    A run counts as failed if any of its rows has an error; failed runs are
    returned by ``load_failures`` instead.
    """
    failed = rows.groupby(["engine", "level_pct", "rep"])["error"].transform(
        lambda errors: errors.notna().any()
    )
    return (
        rows[~failed]
        .groupby(["engine", "level_pct", "rep"], as_index=False)
        .agg(total_s=("time_s", "sum"), peak_gb=("peak_mb", "max"))
        .assign(peak_gb=lambda frame: frame["peak_gb"] / MB_PER_GB)
    )


def load_failures(rows: pd.DataFrame) -> pd.DataFrame:
    """One row per killed run, with the private memory it had committed at the kill."""
    errors = rows[rows["error"].notna()]
    private_mb = errors["error"].str.extract(PRIVATE_AT_KILL, expand=False).astype(float)
    return (
        errors.assign(private_gb=private_mb / MB_PER_GB)
        .groupby(["engine", "level_pct", "rep"], as_index=False)
        .agg(private_gb=("private_gb", "max"), error=("error", "first"))
    )


def summarize(runs: pd.DataFrame) -> pd.DataFrame:
    """Mean, min and max across the finished repetitions of each engine and level."""
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
    axis.set_xlabel("Muestra de datos (porcentaje de las reseñas con texto)", color=TEXT_SECONDARY)


def line_chart(summary: pd.DataFrame, failures: pd.DataFrame, value: str,
               low: str | None, high: str | None, title: str, ylabel: str, unit: str,
               output: Path, memory: bool = False) -> None:
    """One line per engine across the levels, min-max whiskers, and an X per killed run.

    On the time chart a killed run has no time, so its X sits on the top edge
    with a "no terminó" label; on the memory chart it sits at the private
    memory committed when it was killed.
    """
    figure, axis = plt.subplots(figsize=(8, 4.6), dpi=150, facecolor=SURFACE)
    levels = sorted(set(summary["level_pct"]) | set(failures["level_pct"]))
    position = {level: index for index, level in enumerate(levels)}
    # Headroom above the highest whisker for the "no terminó" marks and labels.
    top = summary[high or value].max() * (1.15 if memory else 1.35)
    if memory:
        top = max(top, failures["private_gb"].max() * 1.1 if len(failures) else 0,
                  MACHINE_RAM_GB * 1.12)

    for engine, (label, color) in ENGINES.items():
        data = summary[summary["engine"] == engine].sort_values("level_pct")
        x = [position[level] for level in data["level_pct"]]
        axis.plot(x, data[value], color=color, linewidth=2, marker="o", markersize=7,
                  markeredgecolor=SURFACE, markeredgewidth=2, label=label, zorder=3)
        if low and high:
            axis.vlines(x, data[low], data[high], color=color, linewidth=2, alpha=0.45, zorder=2)
        last = data.iloc[-1]
        lift = 10 if engine == "pandas" else 0  # keeps pandas' label off Spark's line
        axis.annotate(f"{label}: {_es(last[value])} {unit}", (x[-1], last[value]),
                      xytext=(10, lift), textcoords="offset points", va="center", fontsize=9,
                      color=TEXT_PRIMARY)

        killed = failures[failures["engine"] == engine]
        for level, group in killed.groupby("level_pct"):
            heights = group["private_gb"] if memory else [top * 0.85] * len(group)
            axis.scatter([position[level]] * len(group), heights, marker="x", s=60,
                         color=color, linewidths=2, zorder=4)
            note = (f"{label} cortado: {_es(max(heights))} GB comprometidos" if memory
                    else f"{label}: no terminó\n{len(group)}/{len(group)} cortadas por RAM")
            axis.annotate(note, (position[level], max(heights)), xytext=(0, 9),
                          textcoords="offset points", ha="center", fontsize=8,
                          color=TEXT_PRIMARY)

    if memory:
        axis.axhline(MACHINE_RAM_GB, color=TEXT_SECONDARY, linewidth=1, linestyle="--", zorder=1)
        axis.annotate(f"RAM total del equipo: {_es(MACHINE_RAM_GB)} GB",
                      (len(levels) - 1, MACHINE_RAM_GB), xytext=(0, 5), textcoords="offset points",
                      ha="right", fontsize=9, color=TEXT_SECONDARY)
    axis.set_ylim(0, top)
    _level_axis(axis, levels)
    _style(axis, title, ylabel)
    axis.set_xlim(-0.3, len(levels) - 1 + 0.9)
    axis.legend(frameon=False, loc="center left" if memory else "upper left",
                labelcolor=TEXT_PRIMARY)
    figure.tight_layout()
    figure.savefig(output, facecolor=SURFACE)
    plt.close(figure)


def speed_ratio_chart(summary: pd.DataFrame, output: Path) -> pd.Series:
    """Spark time divided by pandas time, only where both engines finished."""
    pivot = summary.pivot(index="level_pct", columns="engine", values="time_mean").sort_index()
    ratio = (pivot["spark"] / pivot["pandas"]).dropna()
    figure, axis = plt.subplots(figsize=(8, 4.2), dpi=150, facecolor=SURFACE)
    x = range(len(ratio))
    axis.plot(x, ratio.values, color=ENGINES["pandas"][1], linewidth=2, marker="o", markersize=7,
              markeredgecolor=SURFACE, markeredgewidth=2, zorder=3)
    for spot, value in zip(x, ratio.values):
        axis.annotate(f"{_es(value)}×", (spot, value), xytext=(0, 9),
                      textcoords="offset points", ha="center", fontsize=9, color=TEXT_PRIMARY)
    axis.axhline(1, color=TEXT_SECONDARY, linewidth=1, linestyle="--")
    axis.annotate("1× = mismo tiempo", (len(ratio) - 1, 1), xytext=(0, -14),
                  textcoords="offset points", ha="right", fontsize=9, color=TEXT_SECONDARY)
    axis.set_ylim(0, ratio.max() * 1.18)
    axis.set_xlim(-0.4, len(ratio) - 1 + 0.4)
    _level_axis(axis, list(ratio.index))
    _style(axis, "Ventaja de pandas donde ambos terminan (tiempo Spark ÷ tiempo pandas)",
           "Veces más rápido pandas")
    figure.tight_layout()
    figure.savefig(output, facecolor=SURFACE)
    plt.close(figure)
    return ratio


def operations_chart(rows: pd.DataFrame, runs: pd.DataFrame, level: float,
                     output: Path) -> pd.DataFrame:
    """Mean time of each operation at one level, both engines side by side."""
    finished = rows.merge(runs[["engine", "level_pct", "rep"]], on=["engine", "level_pct", "rep"])
    data = (
        finished[finished["level_pct"] == level]
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
            axis.annotate(f"{_es(value)} s", (bar.get_width(), bar.get_y() + height / 2),
                          xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                          color=TEXT_SECONDARY)
    axis.set_yticks(list(positions), [OPERATION_NAMES[name] for name in data.index])
    axis.invert_yaxis()
    _style(axis, f"Tiempo medio por operación al {level:g} % de los datos", "")
    axis.grid(axis="y", visible=False)
    axis.grid(axis="x", color=GRID, linewidth=0.8)
    axis.set_xlabel(f"Segundos (media de 3 repeticiones; {level:g} % = mayor muestra que ambos terminan)",
                    color=TEXT_SECONDARY)
    axis.set_xlim(0, data.max().max() * 1.18)
    axis.legend(frameon=False, loc="center right", labelcolor=TEXT_PRIMARY)
    figure.tight_layout()
    figure.savefig(output, facecolor=SURFACE)
    plt.close(figure)
    return data


def write_table(csv_path: Path, summary: pd.DataFrame, failures: pd.DataFrame,
                ratio: pd.Series, operations: pd.DataFrame, output: Path) -> None:
    """Write the numbers behind the charts as Markdown (the accessible table view)."""
    lines = [
        "# Datos de los gráficos",
        "",
        f"Generado por `docs/graficos/make_charts.py` desde `docs/evidencia/{csv_path.name}`.",
        "Tiempo total de una corrida = suma de sus operaciones; pico = el mayor pico entre ellas.",
        "Las corridas cortadas por la guardia de RAM no entran en ningún promedio.",
        f"GB con 1 GB = {MB_PER_GB} MB.",
        "",
        "## Corridas terminadas",
        "",
        "| Muestra | Motor | Corridas | Tiempo medio (s) | Mín (s) | Máx (s) | Pico medio (GB) | Pico máx (GB) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in summary.sort_values(["level_pct", "engine"]).itertuples():
        lines.append(
            f"| {row.level_pct:g} % | {ENGINES[row.engine][0]} | {row.runs} | {row.time_mean:.1f} | "
            f"{row.time_min:.1f} | {row.time_max:.1f} | {row.peak_mean:.2f} | {row.peak_max:.2f} |"
        )
    lines += ["", "## Corridas cortadas (no terminaron)", "",
              "| Muestra | Motor | Rep. | Memoria privada al corte (GB) | Registro |",
              "|---|---|---|---|---|"]
    for row in failures.sort_values(["level_pct", "engine", "rep"]).itertuples():
        lines.append(f"| {row.level_pct:g} % | {ENGINES[row.engine][0]} | {row.rep} | "
                     f"{row.private_gb:.2f} | {row.error} |")
    lines += ["", "## Tiempo Spark ÷ tiempo pandas (donde ambos terminan)", "",
              "| Muestra | Veces |", "|---|---|"]
    lines += [f"| {level:g} % | {value:.1f} |" for level, value in ratio.items()]
    lines += ["", f"## Tiempo medio por operación al {OPERATIONS_LEVEL:g} % (s)", "",
              "| Operación | pandas | PySpark |", "|---|---|---|"]
    for name, row in operations.iterrows():
        lines.append(f"| {OPERATION_NAMES[name]} | {row['pandas']:.1f} | {row['spark']:.1f} |")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    csv_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_CSV
    rows = load_rows(csv_path)
    runs = load_runs(rows)
    failures = load_failures(rows)
    summary = summarize(runs)
    line_chart(summary, failures, "time_mean", "time_min", "time_max",
               "Tiempo total de la canalización según el tamaño de la muestra",
               "Segundos (media de las corridas terminadas)", "s", HERE / "tiempo_total.png")
    line_chart(summary, failures, "peak_mean", None, None,
               "Memoria máxima del proceso según el tamaño de la muestra",
               f"GB, 1 GB = {MB_PER_GB} MB", "GB", HERE / "memoria_pico.png", memory=True)
    ratio = speed_ratio_chart(summary, HERE / "ventaja_tiempo.png")
    operations = operations_chart(rows, runs, OPERATIONS_LEVEL,
                                  HERE / f"tiempo_por_operacion_{OPERATIONS_LEVEL:g}.png")
    write_table(csv_path, summary, failures, ratio, operations, HERE / "datos_graficos.md")
    print("charts written to", HERE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
