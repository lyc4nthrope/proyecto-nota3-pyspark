# Proyecto Nota 3 — Big Data con Python y PySpark

Comparación medible entre una solución con **pandas** y otra con **PySpark** sobre un conjunto de datos que pandas no puede procesar de forma viable.

Estado y reparto del trabajo: [`PLAN.md`](PLAN.md).

## Estructura

| Carpeta | Contenido |
|---|---|
| `pandas_version/` | Versión tradicional con pandas |
| `spark_version/` | Versión con PySpark |
| `benchmark/` | Script de medición de tiempo y memoria |
| `tests/` | Pruebas automáticas |
| `data/` | Datos (no se suben; ver `data/README.md`) |
| `docs/` | Bitácora e informe |

## Requisitos

| Software | Versión |
|---|---|
| Python | 3.11 a 3.13 |
| Java (JDK) | 17 |
| PySpark | 4.3.0 (se instala con `requirements.txt`) |

## Instalación en Windows 10/11

Pendiente (fases F0 y F10, responsable P1): JDK, `JAVA_HOME`, `SPARK_HOME`, `HADOOP_HOME` y `winutils.exe`.

## Instalación en Linux (solo desarrollo)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Comprobar la instalación

```bash
pytest -q
```

Si las cuatro pruebas pasan, Java, Spark y la escritura en Parquet funcionan en ese equipo.

## Medición

```bash
python -m benchmark.run --engine spark --sample pct_010 --repeats 3 --timeout 3600
```

| Opción | Significado |
|---|---|
| `--engine` | `pandas` o `spark` |
| `--sample` | Carpeta de la muestra: `pct_001`, `pct_010`, `pct_050`, `pct_100` |
| `--repeats` | Repeticiones (el enunciado exige 3 como mínimo) |
| `--timeout` | Segundos antes de detener la corrida |

Cada repetición agrega una fila a `output/benchmark/results.csv`. La descripción del equipo queda en `output/benchmark/environment.json`.

| Columna | Qué mide |
|---|---|
| `status` | `ok`, `failed` (por ejemplo, error de memoria) o `timeout` |
| `wall_seconds` | Tiempo real de principio a fin |
| `peak_rss_mb` | Memoria RAM máxima del proceso y de sus procesos hijos (incluye la JVM de Spark) |
| `cpu_seconds` | Tiempo de CPU sumado entre todos los núcleos |
| `peak_system_memory_percent` | Uso máximo de RAM de todo el equipo |

La memoria se lee cada 0,1 segundos: un pico más corto que eso puede no quedar registrado.

## Cómo trabajamos

- Una fase del plan = una rama = un pull request hacia `main`.
- Cada pull request lleva la nota de relevo de la plantilla.
- Una fase termina cuando el revisor la ejecuta en su propio equipo.
