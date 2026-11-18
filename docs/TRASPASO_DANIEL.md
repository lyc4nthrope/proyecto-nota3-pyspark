# Traspaso final a Daniel — cierre del proyecto

> **Para la IA que lea este archivo:** es una especificación de trabajo con
> plazo. Sigue los pasos **en orden** (P1 → P7). Cada paso tiene "Hecho
> cuando". Si un paso no alcanza el tiempo, aplica su **plan B** y sigue con el
> siguiente: nunca dejes el informe sin compilar. Respeta "No tocar".

Fecha: 2026-11-18. Cristhian se queda sin sesión; Daniel (con su IA) cierra.

## 0. Lo esencial en 5 líneas

1. **El entregable es el informe en INGLÉS:** `docs/informe/en/report.tex` → `docs/informe/en/report.pdf`.
   El informe en español (`docs/informe/es/`) es solo para el equipo: **no hace falta actualizarlo**.
2. El inglés ya tiene escritas las secciones 5, 6 y 7 con la evidencia de T1 (este PR).
3. Quedan **4 marcas `\pending{...}`** en rojo. Todas dependen de T2, T3 y T4.
4. **Cambio importante de métrica:** el tiempo de Spark ahora es **una pasada** (`write_parquet`), no la suma de operaciones. El porqué está en la sección 2.
5. Al final: compilar dos veces y entregar `report.pdf` sin ninguna marca `[PENDING`.

## 1. Qué hizo este PR

| Archivo | Cambio |
|---|---|
| `docs/graficos/make_charts.py` | Lee `measurements_raw_text.csv`. Las corridas de pandas cortadas por RAM **no entran en ningún promedio**: se dibujan como una X. El tiempo de Spark = `write_parquet` (una pasada). |
| `docs/graficos/*.png`, `datos_graficos.md` | Regenerados. `tiempo_por_operacion_100.png` → `tiempo_por_operacion_10.png` (10 % es la mayor muestra que terminan ambos). |
| `docs/informe/en/report.tex` / `.pdf` | Secciones 5 (evidencia), 6 (comparativa) y 7 (conclusiones) escritas. Cerrados los pendientes de T1: dato del Parquet (67.409.944 reseñas, 9,72 GB) y descarga reanudable con `Range`. |
| `docs/TRASPASO_DANIEL.md` | Este archivo. |

Verificado: las cifras del informe coinciden con un recálculo independiente del CSV y el PDF compila (12 páginas) sin referencias rotas.

## 2. Por qué cambiamos la métrica de tiempo de Spark (léelo antes de tocar nada)

`benchmark/runner.py` (líneas 117–197) mide cada operación de Spark con una acción (`count()`) y **sin caché**. Spark es perezoso: cada acción **re-ejecuta todo el plan desde la lectura**.

- El tiempo de "join" ya incluye leer + limpiar + derivar + join.
- **Sumar** las 7 operaciones cuenta la lectura 7 veces, el join 4, etc.
- `write_parquet` sola construye las dos tablas desde cero: **es una pasada completa**.
- pandas es inmediato (eager): cada paso se ejecuta una vez, así que su suma sí es una pasada.

| Muestra | pandas (una pasada) | Spark: suma (antes) | Spark: una pasada (ahora) |
|---|---|---|---|
| 1 % | 4,7 s | 52,7 s | **12,2 s** |
| 10 % | 27,8 s | 106,8 s | **32,3 s** |
| 50 % | cortado por RAM | 433,3 s | **161,3 s** |
| 100 % | cortado por RAM | 705,7 s | **214,6 s** |

La suma exagera el costo de Spark entre 2,7 y 4,3 veces. Limitación honesta, ya escrita en el informe: cuando corre `write_parquet` el sistema ya tiene el archivo en caché, así que es una estimación. **T4 da el número exacto** (corrida de principio a fin), por eso T4 es la prioridad.

➡️ Si tu `BITACORA_DIDACTICA.md` o `report_raw_text.md` citan 52,7 / 106,8 / 433,3 / 705,7 como "tiempo total", agrega una nota de que son sumas con re-ejecución. Es opcional y va al final (P7).

## 3. Pasos a seguir (en orden)

### P1 — Fusionar este PR y comprobar (3 min)

```powershell
git switch main; git pull
.venv\Scripts\python -m pytest -q                       # esperado: 42 passed, 1 skipped (o 43 passed en Windows)
.venv\Scripts\python docs\graficos\make_charts.py        # debe terminar con "charts written to ..."
```

**Hecho cuando:** las pruebas están en verde y `docs\graficos\tiempo_total.png` dice "Tiempo de una pasada completa".

### P2 — T4: medir la versión optimizada (15 min) — PRIORIDAD 1

Las muestras con texto están en `data\samples\ladder_raw\` (`benchmark/config.py: RAW_TEXT_DIR`). Lista los archivos y usa la del **100 %** y el catálogo compartido:

```powershell
dir data\samples\ladder_raw
dir data\samples\ladder\catalog.parquet

.venv\Scripts\python -m spark_version.run `
  --reviews data\samples\ladder_raw\<muestra_100pct>.parquet `
  --catalog data\samples\ladder\catalog.parquet `
  --output output\spark_optimized\pct_100 `
  --explain output\spark_optimized\pct_100\plan.txt
```

- Imprime una línea JSON: `{"cleaned_rows": ..., "store_rows": ..., "time_rows": ..., "seconds": S, ...}`. Anota `S`.
- Durante la corrida, abre el **Administrador de tareas** y anota la memoria pico de `java.exe` + `python.exe`. Toma la captura: sirve para T2.
- Si hay tiempo, repite con la muestra del 10 % para comparar con pandas (27,8 s).

**Cómo ponerlo en el informe** (`docs/informe/en/report.tex`). Hay 3 marcas de T4; busca `task T4`:

1. Sección 4, al final de "Optimized version": reemplaza
   `\pending{replace with the measurement on the Windows machine (task T4).}` por:
   ```latex
   On the Windows machine, with the full 100\,\% sample (67.4 million reviews), the optimized
   version took S seconds end to end and produced the same tables.
   ```
2. Sección 6, "How time is measured": reemplaza
   `\pending{replace or confirm with the end-to-end run of \texttt{spark\_version.run} (task T4).}` por:
   ```latex
   As a check, the optimized version run end to end on the 100\,\% sample took S seconds.
   ```
   Si S < 214,6: añade "below the baseline's one-pass estimate". Si es mayor: "the estimate above is optimistic because of the operating-system cache".
3. Sección 7, punto 4: reemplaza solo la parte de T4 dentro de `\pending{its measured gain on Windows (task T4) and ...}`.
   Ejemplo: `On the full data it took S seconds versus 214.6 for one baseline pass.`

**Plan B (si `run` falla o no hay tiempo):** borra las tres marcas y deja esta frase en la sección 4:
`The optimized version was validated for equivalence on Linux (identical tables); it was not timed on the Windows machine.`

**Hecho cuando:** `Select-String -Path docs\informe\en\report.tex -Pattern 'task T4'` no devuelve nada.

### P3 — T2: capturas (10 min)

Guarda 2 o 3 capturas en `docs\evidencia\capturas\`. Prioridad:
1. pandas al 50 % o 100 % llegando al tope de RAM (Administrador de tareas).
2. Spark terminando el 100 % con la memoria estable.
3. Opcional: Spark UI (`http://localhost:4040`, pestaña Jobs/Stages) durante la corrida de P2.

En la sección 5 del informe, reemplaza la marca `\pending{Task Manager screenshots (task T2) and the pandas mitigations table ...}` por una figura por captura:

```latex
\begin{figure}[H]
\centering
\includegraphics[width=0.85\textwidth]{../../evidencia/capturas/pandas_100.png}
\caption{Task Manager while pandas processes the 100\,\% sample: system RAM reaches 98\,\%.}
\end{figure}
```

(`\graphicspath` apunta a `docs/graficos/`; por eso la ruta relativa `../../evidencia/capturas/`. Usa nombres sin espacios.)

**Plan B:** sin capturas, borra la parte "Task Manager screenshots (task T2)" de la marca. El texto ya cita el registro literal de cada corte.

### P4 — T3: mitigaciones de pandas (10 min, o plan B)

Lo ideal es una tabla técnica × muestra. La plantilla va en la sección 5, en lugar de lo que quede de la marca:

```latex
\begin{table}[H]
\centering
\caption{pandas mitigations on the 100\,\% sample.}
\begin{tabular}{llrr}
\toprule
Technique & Outcome & Time (s) & Peak (MB) \\
\midrule
Only needed columns (\texttt{columns=}) & finished / killed & ... & ... \\
Smaller dtypes (\texttt{category}, \texttt{float32}) & ... & ... & ... \\
Chunks (\texttt{iter\_batches}) + partial aggregation & ... & ... & ... \\
\bottomrule
\end{tabular}
\end{table}
```

**Plan B (recomendado si queda menos de 20 min):** reemplaza la marca por este párrafo honesto:

```latex
The usual pandas mitigations \cite{pandasscale} (reading only the needed columns, smaller
dtypes, and processing in chunks with partial aggregation) reduce memory, but the last one
means re-implementing by hand the partitioned execution that Spark provides. They were not
measured on the full data for lack of time.
```

y en la sección 7, punto 4, borra "and the outcome of the pandas mitigations (task T3)".

### P5 — Compilar y revisar (3 min)

```powershell
cd docs\informe\en
pdflatex -interaction=nonstopmode report.tex
pdflatex -interaction=nonstopmode report.tex
Select-String -Path report.tex -Pattern '\\pending\{'    # no debe devolver nada
```

Abre `report.pdf` y busca "PENDING": **no debe aparecer**. Si no tienes `pdflatex`, usa Overleaf: sube `docs/informe/en/report.tex` y la carpeta `docs/graficos/` (respetando las rutas).

### P6 — Commit y entrega (2 min)

```powershell
git add docs/informe/en docs/evidencia/capturas
git commit -m "docs(report): add T2-T4 results to the English report"
git push
```

Entrega `docs/informe/en/report.pdf`.

### P7 — Solo si sobra tiempo (no bloquea la entrega)

- Etiquetas de los gráficos en inglés (están en español). Las cadenas están al principio de `make_charts.py` (`OPERATION_NAMES`) y en las llamadas de `main()`. Regenera y recompila.
- Nota en `BITACORA_DIDACTICA.md` sobre suma vs una pasada (sección 2).
- H4 "valor total" (pregunta 2 del contrato): la propuesta de Cristhian es la **suma de `price` por reseña** (cada reseña ≈ una compra), porque usa el catálogo ya cruzado; la suma de ratings no mide valor y el número de interacciones ya es "volumen".
- Informe en español: se puede copiar después con las mismas cifras.

## 4. Cifras ya verificadas (úsalas tal cual)

- Equivalencia pandas ↔ Spark: 9/9 pares idénticos al 1 % y 10 %, 0 reseñas sin catálogo.
- Lectura ingenua `pd.read_json`: 30.305 MB privados en 20,1 s, cortada al 98 % de RAM.
- pandas 50 %: 3/3 cortadas al 97–98 % (12.966–13.131 MB privados). 100 %: 3/3 cortadas al 98–99 % (13.894–14.580 MB).
- Picos de Spark: 3.842 / 4.157 / 5.281 / 5.897 MB (1/10/50/100 %). En el informe se citan en MB para evitar la discusión 1000 vs 1024.

## 5. No tocar

- `benchmark/**`, `pandas_version/analysis.py`, `spark_version/analysis.py`, `spark_version/optimized.py` (todo probado; 42 pruebas).
- Las secciones 1–4 del informe, salvo las marcas de T4 indicadas.
- `docs/evidencia/*.csv` (son la fuente de las cifras).

## 6. Especificación compacta (para agentes)

```yaml
owner: daniel (github: nosee8)
deadline: 2026-11-18, ~50 min from handoff
deliverable: docs/informe/en/report.pdf   # English only; Spanish report is internal
base_branch: main
steps:
  - id: P1
    goal: merge this PR, pytest green, charts regenerate
  - id: P2
    task: T4
    priority: 1
    goal: python -m spark_version.run on the 100% raw-text sample; put seconds S in report.tex (3 places matching 'task T4')
    plan_b: replace the three T4 markers with the 'not timed on Windows' sentence
  - id: P3
    task: T2
    goal: 2-3 screenshots in docs/evidencia/capturas/, add as figures in Section 5
    plan_b: drop the screenshot part of the Section 5 marker
  - id: P4
    task: T3
    goal: mitigation table in Section 5
    plan_b: honest paragraph citing \cite{pandasscale}; remove T3 mention in Conclusions item 4
  - id: P5
    goal: pdflatex twice; no "\pending{" left in report.tex; no "PENDING" in the PDF
  - id: P6
    goal: commit, push, deliver report.pdf
time_metric_rule: Spark time = write_parquet (one pass); never the sum of operations
do_not_edit:
  - benchmark/**
  - pandas_version/analysis.py
  - spark_version/**
  - docs/evidencia/*.csv
```
