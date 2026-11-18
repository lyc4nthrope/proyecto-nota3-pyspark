# Benchmark — Amazon Home_and_Kitchen

Herramientas de medición para el dataset
`McAuley-Lab/Amazon-Reviews-2023` (config `Home_and_Kitchen`). **No son el
pipeline final y no aplican optimizaciones**: no usan `cache`, ni *broadcast
hints*, ni reparticionado, ni configuración de *shuffle* distinta de la
predeterminada. Solo miden.

Hay dos familias de muestra, con propósitos distintos:

| Familia | Qué es | Para qué |
|---|---|---|
| **Perfil** (sonda) | Clúster de productos: 1 % de bytes del metadata en 200 ventanas deterministas + todas las interacciones 0core de esos productos (~671 mil filas) | Diagnosticar los datos sin bajar los 43 GB crudos |
| **Escalera** (comparación) | Fracciones deterministas de filas por *hash* (1 %, 10 %, 50 %, 100 %) cruzadas contra el **catálogo completo** de productos (3,7 M) | Comparar pandas vs PySpark a medida que crecen los datos |

## 1. Sonda de perfil

```powershell
.venv\Scripts\python -m benchmark.measure_sample
```

Baja la tabla publicada `benchmark/0core/rating_only/Home_and_Kitchen.csv`
(3,86 GB, 67,4 M filas: `user_id`, `parent_asin`, `rating`, `timestamp`),
muestrea el 1 % de bytes del metadata JSONL en ventanas alineadas a líneas, y
conserva todas las interacciones de esos productos.

Mide: perfil de la muestra (filas, nulos, distribución de ratings, cobertura
temporal) y, una por una, lectura, limpieza, columnas derivadas, join con el
catálogo, agregación por tienda, agregación temporal y escritura Parquet.
Registra tiempo, RSS pico del árbol de procesos (Python + JVM) y errores.

Salidas en `output/measurements/`: `report.md`, `measurements.csv`, `run.json`
y las tablas agregadas en `tables/spark/`.

## 2. Comparación pandas vs PySpark (escalera)

```powershell
# todo: prepara muestras, corre motor x nivel x repeticiones y reporta
.venv\Scripts\python -m benchmark.compare

# solo preparar el catálogo completo y las muestras de cada nivel
.venv\Scripts\python -m benchmark.compare --build-only

# una corrida suelta (la usa el orquestador)
.venv\Scripts\python -m benchmark.compare --single-run --engine pandas --level 0.1 --rep 1
```

Opciones: `--levels 0.01,0.1,0.5,1.0`, `--engines pandas,spark`, `--repeat 3`,
`--driver-memory 4g`, `--timeout 3600` (segundos por corrida), `--skip-build`.

Cómo se construye la escalera:

1. **Catálogo completo, una vez**: se baja el metadata JSONL (11,8 GB) y se
   proyecta a un Parquet con las 6 columnas del análisis. Ambos motores leen
   ese mismo archivo, así el join cuesta lo mismo en cada uno.
2. **Filas por nivel**: se selecciona un fracción de filas de la tabla de
   interacciones con `xxhash64(user_id, parent_asin, rating, timestamp)`, un
   *hash* determinista que da la misma muestra en cualquier máquina sin
   descargar nada más. La fracción real se mide y se informa.
3. **Una corrida = un proceso**: cada combinación de motor, nivel y repetición
   corre en un proceso nuevo. El allocator de pandas y la JVM conservan
   memoria liberada, así que repeticiones dentro de un mismo proceso medirían
   las corridas anteriores, no la actual.

Salidas en `output/measurements/`: `report_ladder.md`,
`measurements_ladder.csv`, `run_ladder.json` y `runs/` (una corrida por JSON).
Las tablas agregadas van en `tables/pandas/` y `tables/spark/`, separadas
porque Spark escribe directorios de partes y pandas archivos sueltos.

## Escenarios de ejecución

### Primera vez (en frío)

Todo se crea solo, en este orden: tabla de interacciones (3,86 GB), metadata
completo (11,8 GB, solo hace falta para la comparación), catálogo proyectado
(~64 MB) y una muestra por nivel (28,7 MB al 1 %, 245 MB al 10 %, 978 MB al
50 %, 1,7 GB al 100 %). La sonda usa su propio trozo de metadata (~0,3 GB
descargados, 113 MB conservados); el CSV de interacciones es compartido, así
que ejecutar ambas herramientas no duplica esa descarga.

### Repetición

Los mismos comandos **reutilizan todo** lo ya procesado: cada descarga se
valida por tamaño y cada muestra/catálogo por su manifiesto
(`.manifest.json`, que guarda la versión del algoritmo que lo generó).
Cambiar código no reconstruye nada; `--skip-build` salta directo a las
corridas.

### Datos ya procesados

Con estos artefactos presentes, **todo funciona sin red**:

| Artefacto | Ruta | Lo crea | Se reutiliza si |
|---|---|---|---|
| Interacciones (0core) | `data/raw/amazon_hk/Home_and_Kitchen.0core.rating_only.csv` | primera ejecución | el tamaño coincide |
| Metadata completo | `data/raw/amazon_hk/meta_Home_and_Kitchen_full.jsonl` | la comparación | el archivo existe y pesa lo esperado |
| Catálogo proyectado | `data/samples/ladder/catalog.parquet` | `--build-only` | su manifiesto coincide |
| Muestra por nivel | `data/samples/ladder/reviews_<nivel>.parquet` | `--build-only` | su manifiesto coincide |
| Muestra de perfil (sonda) | `data/samples/amazon_hk_reviews_1pct.parquet` | la sonda | su manifiesto coincide |
| Catálogo de la sonda | `data/samples/amazon_hk_catalog.parquet` | la sonda | su manifiesto coincide |

Las **salidas** (`output/measurements/…`) se regeneran en cada corrida: no
son caché, son resultados.

### Forzar una reconstrucción

```powershell
# sonda: vuelve a descargar y a muestrear
.venv\Scripts\python -m benchmark.measure_sample --force-download --rebuild-sample

# comparación: borrar el manifiesto del artefacto que se quiera regenerar
Remove-Item data\samples\ladder\reviews_10pct.manifest.json
.venv\Scripts\python -m benchmark.compare --build-only --levels 0.1
```

## Cómo interpretar las tablas

- Los tiempos de Spark son **acumulativos**: sin `cache`, la operación N
  vuelve a ejecutar 1..N. La suma de tiempos no es el tiempo de un pipeline.
- El RSS pico es la memoria residente total del proceso Python y sus hijos JVM,
  muestreada cada 50 ms; no es el *heap* de la JVM y puede bajar entre
  operaciones. Compare la columna **Delta**, no los picos absolutos.
- **Frame (MB)** (solo pandas) es el tamaño en memoria profunda del DataFrame
  principal: es el número que decide si cabe en la RAM del equipo.
- Un error se registra en la columna `Error` y sus operaciones dependientes
  quedan como `skipped`; la corrida no se aborta. Una corrida que supera el
  *timeout* se registra como tal.
- Un *timeout* en pandas es evidencia, no un fallo de la herramienta: significa
  que esa operación no terminó en el tiempo razonable asignado.

## Estructura del código

| Módulo | Responsabilidad |
|---|---|
| `benchmark/config.py` | Qué datos se miden y dónde se escriben los resultados |
| `benchmark/sources.py` | Descarga HTTP y muestreo determinista por ventanas de bytes |
| `benchmark/instrument.py` | El instrumento: tiempo, RSS pico y secuencia de operaciones |
| `benchmark/profile.py` | Diagnóstico de la muestra |
| `benchmark/ladder.py` | Catálogo compartido y muestras por fracción de filas |
| `benchmark/runner.py` | Orquestación de operaciones Spark |
| `benchmark/pandas_runner.py` | Orquestación de operaciones pandas |
| `benchmark/compare.py` | Escalera: construcción, corridas aisladas y reporte |
| `benchmark/report.py` | Render Markdown/CSV/JSON |
| `benchmark/measure_sample.py` | CLI de la sonda de perfil |
| `spark_version/analysis.py` | Transformaciones Spark entregables |
| `pandas_version/analysis.py` | Transformaciones pandas entregables |

Lo medido es lo entregado: los runners ejecutan las funciones de
`spark_version.analysis` y `pandas_version.analysis`, que son las mismas que
usará el pipeline final. Las pruebas de equivalencia
(`tests/test_analysis_equivalence.py`) fijan que ambos motores producen los
mismos resultados, incluida la numeración de días de la semana de Spark.
