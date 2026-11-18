# Tareas para Cristhian — cierre en paralelo

> **Para la IA que lea este archivo:** es una especificación de trabajo. Ejecuta
> las tareas **en el orden de prioridad**, respeta la sección "Archivos que
> puedes tocar" y cumple cada "Hecho cuando" antes de pasar a la siguiente.

Fecha: 2026-11-18. Espejo de `docs/TAREAS_DANIEL.md`: mientras Daniel cierra
T2 (capturas), T3 (mitigaciones de pandas) y T4 (medición de la versión
optimizada), estas tareas se hacen **sin esperar a nadie**.

## Material disponible ya (no esperes a nada)

Todo está en el PR #10 y en `docs/evidencia/` de su rama:

- `docs/evidencia/report_raw_text.md` — tabla por nivel × motor × operación
  (tiempos, picos de memoria, errores).
- `docs/evidencia/measurements_raw_text.csv` — datos crudos para gráficos.
  Columnas: `level_pct`, `engine`, `rep`, `operation`, `result`, `time_s`,
  `peak_mb`, `baseline_mb`, `delta_mb`, `rss_mean_mb`, `frame_mb`, `error`.
- **Equivalencia verificada: 9/9 pares idénticos** entre motores al 1 % y 10 %
  (mismos grupos, mismo top-1, 0 filas sin catálogo). Se puede afirmar tal cual.
- Frase de inviabilidad ya redactada: *el intento ingenuo de leer los 31 GB con
  pandas comprometió **30,3 GB de memoria privada en 20 segundos** sobre un
  equipo de 15,3 GB, y el sistema cortó al 98 % de RAM*.
- Método de medición con salvaguardas: guardia de RAM (mata el árbol completo
  antes de congelar la máquina), enfriamiento entre corridas, bytes privados
  registrados al corte y una corrida por proceso. Es material para la sección
  de metodología.

## Prioridades

| # | Tarea | Prioridad | Sin esto… |
|---|---|---|---|
| H1 | Reescribir narrativa y secciones 5–7 del informe con la evidencia nueva | **Crítica** | El informe cuenta la historia vieja (pandas terminaba) |
| H2 | Regenerar los gráficos con `measurements_raw_text.csv` | **Crítica** | Los gráficos contradicen el texto |
| H3 | Preparar plantillas para T3 (tabla técnica × nivel) y T4 (`spark_optimized`) | Alta | Última hora con formato por decidir |
| H4 | Proponer la definición de "valor total" (pregunta 2 del contrato) | Alta | El contrato no cierra |
| H5 | Revisar el PR #10 en tu equipo (revisión rotativa) | Media | La convención del plan queda sin cumplir |

## H1 — Narrativa y secciones 5–7

El cambio de historia: sobre la tabla **sin texto**, pandas terminaba el 100 %
en ~205 s y el volumen no justificaba Spark. Sobre la tabla **con texto** (los
hechos reales del análisis), pandas **no termina** ni al 50 %: las corridas
grandes son cortadas al 97–99 % de RAM del sistema. Spark termina los cuatro
niveles (52,7 s / 106,8 s / 433,3 s / 705,7 s; picos de 3,8 a 5,9 GB).

**Hecho cuando:** las secciones 5–7 citan únicamente cifras de
`measurements_raw_text.csv` (o de la tabla de arriba) y ninguna de la escalera
antigua.

## H2 — Gráficos

```powershell
.venv\Scripts\python docs\graficos\make_charts.py docs\evidencia\measurements_raw_text.csv
```

⚠️ **Cuidado con las filas de fallo:** en los niveles 50 % y 100 % las corridas
de pandas aparecen como **una sola operación `run` con error y sin tiempo**. Si
`load_runs` las suma como corridas normales, los promedios salen en cero y el
gráfico miente. Exclúyelas del tiempo total o muéstralas como "no terminó".

**Hecho cuando:** ningún gráfico muestra promedios de pandas al 50/100 % y los
títulos reflejan la ruta con texto.

## H3 — Plantillas para lo que viene

- Tabla **técnica × nivel** de T3: columnas `técnica`, `nivel`, `terminó /
  falló / timeout`, `tiempo (s)`, `memoria pico (MB)`.
- En los gráficos comparativos, hueco para `engine = spark_optimized` (T4) al
  lado de `spark` y `pandas`.

**Hecho cuando:** las plantillas existen con las columnas finales y solo falta
rellenar filas.

## H4 — "Valor total" del contrato

La pregunta 2 habla de "mayor volumen y mayor valor total". En este dominio las
opciones son: **suma de ratings**, **número de interacciones** o **precio medio
ponderado**. Trae tu propuesta con una frase de justificación y la cerramos los
tres.

## H5 — Revisión del PR #10

La revisión rotativa que exige el plan (y la sustentación). Verifica en tu
equipo: `pytest -q` en verde y que `docs/evidencia/` corresponda al CSV.

## Qué estamos haciendo nosotros (para que no lo toques)

| Daniel (tú) | Daniel + IA |
|---|---|
| T2: 5 capturas del Administrador de tareas y la interfaz de Spark | T3: `pandas_version/mitigated.py` y la tabla técnica × nivel |
| | T4: medición de `spark_version/run.py` (filas `spark_optimized`) |
| | Cierre de puntos del contrato y reposición de evidencia |

`docs/evidencia/capturas/` es nuestro; tus archivos siguen siendo
`docs/informe/*` y `docs/graficos/*`.

## Especificación compacta (para agentes)

```yaml
owner: cristhian (github: lyc4nthrope)
base_branch: main
do_not_edit:
  - benchmark/**
  - pandas_version/**
  - docs/evidencia/capturas/**
  - docs/CONTRATO_ANALISIS_amazon_hk.md
tasks:
  - id: H1
    priority: critical
    goal: rewrite report sections 5-7 from docs/evidencia/measurements_raw_text.csv only
  - id: H2
    priority: critical
    goal: regenerate charts with the raw-text CSV; exclude failed pandas rows at 50/100% from averages
  - id: H3
    priority: high
    goal: templates for T3 technique-by-level table and spark_optimized chart slots
  - id: H4
    priority: high
    goal: propose the 'total value' definition for business question 2
  - id: H5
    priority: medium
    goal: rotated review of PR #10 on your own machine
evidence_available:
  - docs/evidencia/report_raw_text.md
  - docs/evidencia/measurements_raw_text.csv
verified_facts:
  - equivalence: 9/9 identical engine pairs at 1% and 10%
  - naive read: 30.3 GB private in 20 s before the 98% RAM kill
  - spark finishes all levels; pandas is killed at 50% and 100%
```
