# Tareas para Daniel — cierre del Proyecto Nota 3

> **Para la IA que lea este archivo:** es una especificación de trabajo. Ejecuta las tareas **en el orden de prioridad**, respeta la sección "Archivos que puedes tocar" para no chocar con el otro integrante y cumple cada "Hecho cuando" antes de pasar a la siguiente. Si se acaba el tiempo, entrega lo terminado: **T1 sola ya salva la nota**.

Fecha: 2026-11-17. Equipo efectivo: **Daniel (Windows, datos, mediciones)** y **Cristhian (Linux: Spark optimizado, gráficos e informe)**. La entrega es inminente: se acepta un resultado incompleto si queda documentado.

## Por qué hace falta esto

Tu PR #3 (ya fusionado en `main`) demuestra que, con el CSV `0core/rating_only` (3,86 GB), **pandas termina el 100 %** en unos 205 s con 10,2 GB de pico. La rúbrica pone 0–2,9 en código si "el volumen de datos no justifica el uso de Spark", y el enunciado exige evidencia de que pandas no es viable. **Falta un volumen que haga fallar a pandas** en tu equipo de 15,3 GB de RAM.

## Resumen de prioridades

| # | Tarea | Prioridad | Tiempo máximo | Sin esto… |
|---|---|---|---|---|
| T0 | Enviar a Cristhian la muestra del 1 % y el catálogo en Parquet | **Ya** | 10 min | Cristhian no puede probar el Spark optimizado |
| T1 | Hechos = JSONL crudo de reseñas (31,4 GB) con texto; pandas debe fallar | **Crítica** | 3 h | Nota de código entre 0 y 2,9 |
| T2 | Capturas del Administrador de tareas (fallo de pandas y Spark terminando) | **Crítica** | 15 min | Falta la evidencia que pide el punto 5 |
| T3 | Mitigaciones de pandas (F4): columnas, tipos, *chunks* | Alta | 1,5 h | El informe no puede afirmar que pandas es inviable "aun mitigado" |
| T4 | Correr la escalera con la versión Spark optimizada de Cristhian | Media | 1 h | Solo hay números de Spark sin optimizar |

## Archivos que puedes tocar

| Daniel (tú) | Cristhian (no tocar) |
|---|---|
| `benchmark/*` | `spark_version/optimized.py` (nuevo) |
| `pandas_version/*` | `spark_version/run.py` (nuevo) |
| `spark_version/analysis.py` (solo para la columna nueva de T1) | `tests/test_spark_optimized.py` (nuevo) |
| `docs/evidencia/*`, `docs/CONTRATO_ANALISIS_amazon_hk.md` | `docs/informe/*` (nuevo) |
| `docs/BITACORA.md` (agregar filas al final) y `docs/BITACORA_DIDACTICA.md` (acciones, comandos y librerías) | `docs/graficos/*` (nuevo) |

Cristhian programa la versión optimizada **importando** las funciones de `spark_version/analysis.py` (`clean_reviews`, `add_derived_columns`, `join_with_catalog`, `aggregate_by_store`, `aggregate_by_time`, `write_outputs`). **No cambies sus nombres ni sus parámetros**; agregar columnas a las salidas sí está permitido.

## T0 — Compartir datos con Cristhian (ya)

Cristhian tiene 13 GB libres y no puede bajar el catálogo JSONL de 11,8 GB.

1. Comprime y súbelo a Drive:
   - `data/samples/ladder/catalog.parquet`
   - La muestra del 1 % de la escalera (`level_sample_path(0.01)` en `benchmark/ladder.py`).
2. Pásale el enlace por WhatsApp.
3. Cuando exista la muestra del 1 % de T1 (con texto), compártela igual.

**Hecho cuando:** Cristhian confirma que descargó los archivos.

## T1 — Volumen que haga fallar a pandas (crítica)

**Idea:** usar como hechos el JSONL **crudo** de reseñas, que trae el texto. El texto es lo que multiplica la memoria en pandas (cadenas de Python); en Spark también pesa, pero muchísimo menos (cadenas compactas por particiones, con *spill* a disco cuando no caben).

| Archivo | URL | Tamaño (verificado 2026-11-17) |
|---|---|---|
| Reseñas crudas | `https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/Home_and_Kitchen.jsonl` | 31,41 GB |

Pasos:

1. **Descarga** (agrega `REVIEW_JSONL_URL` en `benchmark/config.py`). **Ojo: `benchmark/sources.py` no es reanudable hoy**: `_download_once` borra el `.part` al fallar y `download_full` reintenta desde cero, así que una caída de red al 90 % cuesta re-descargar los 31 GB. Antes de bajar, dale reanudación (conservar el `.part` y continuar con `Range: bytes=<tamaño_del_part>-`) o descarga por ventanas con `fetch_range`.
2. **Conversión única con Spark a Parquet**, con **esquema explícito** (nunca inferido: inferir lee los 31 GB dos veces). Columnas: `user_id`, `parent_asin`, `rating`, `timestamp`, `text`, `title`, `helpful_vote`, `verified_purchase`.
3. **Columna nueva en el análisis**, igual en pandas y Spark: `text_length = número de caracteres de text` (nulo → 0). En las dos tablas de salida agregar `avg_text_length` (media, 4 decimales). Actualizar el contrato (secciones 2 y 6) y las pruebas de equivalencia.
4. **Escalera 1/10/50/100 %** con el mismo hash determinista que ya usas. El enunciado pide **≥ 3 repeticiones** por corrida; el ahorro de tiempo va solo en Spark:
   - 3 repeticiones en 1 % y 10 % (ambos motores) y en pandas al 50 % y 100 % (si falla, muere pronto y repetir es barato).
   - **1 repetición en Spark al 50 % y 100 %**, y anotar en el reporte qué corridas llevan una sola repetición y por qué (tiempo de entrega).
5. **Intento ingenuo directo** (es la evidencia más clara): `pd.read_json(ruta_jsonl, lines=True)` sobre el archivo completo, con el instrumento midiendo. **Ejecútalo como subproceso con timeout** (15–20 min, como las corridas del orquestador): sin él, un pandas que se va a la paginación puede dejar la máquina inutilizable durante horas en vez de fallar. Se espera `MemoryError`, cierre del proceso, timeout o RAM cerca del 100 %; registra el error textual.
6. Regenerar `docs/evidencia/report_*.md` y el CSV de mediciones.

**Hecho cuando:**
- Existe al menos una corrida de pandas con resultado `failed`/`MemoryError`/proceso terminado **o** con RAM del sistema ≥ 95 %, registrada en el CSV.
- Spark termina el 100 % y escribe las dos tablas en Parquet.
- En los niveles donde ambos terminan, las salidas son equivalentes (criterios de la sección 8 del contrato).
- `pytest -q` pasa.

**Si a las 3 h no está:** sube lo que tengas y escribe en el PR hasta dónde llegó y por qué. El informe usará la escalera actual y explicará la limitación.

## T2 — Capturas (crítica, 15 min)

Con el Administrador de tareas abierto en la pestaña **Rendimiento → Memoria**. Si el intento ingenuo muere demasiado rápido para capturarlo, toma las capturas 1 y 2 durante la corrida medida del 100 % de la escalera, que también llega al límite:

| Captura | Momento | Nombre del archivo |
|---|---|---|
| 1 | pandas en el pico, justo antes o durante el fallo | `docs/evidencia/capturas/pandas_fallo_memoria.png` |
| 2 | El mensaje de error en la terminal | `docs/evidencia/capturas/pandas_error_terminal.png` |
| 3 | Spark procesando el 100 % | `docs/evidencia/capturas/spark_100_memoria.png` |
| 4 | Spark terminado (salida de la terminal) | `docs/evidencia/capturas/spark_100_terminal.png` |
| 5 | Interfaz de Spark `http://localhost:4040`, pestaña **SQL / DataFrame** de la corrida del 100 % | `docs/evidencia/capturas/spark_ui_dag.png` |

**Hecho cuando:** las 5 imágenes están en la rama.

## T3 — Mitigaciones de pandas (F4, alta)

El enunciado exige intentar primero las técnicas propias de pandas y explicar por qué no bastan. Implementar en `pandas_version/mitigated.py`, una función por técnica, sobre los hechos de T1:

| Técnica | Qué hacer | Qué anotar |
|---|---|---|
| Selección de columnas | Leer solo las columnas que usa el análisis | Memoria y nivel máximo que termina |
| Tipos optimizados | `category` para `store`/`main_category`, `float32` para `rating`, `int8` para las partes de fecha | Ídem |
| Lectura por *chunks* | `read_json(..., lines=True, chunksize=N)` con agregación parcial: sumas, conteos y suma de cuadrados para la desviación | Tiempo total y qué **no** se puede hacer por *chunks* (por ejemplo, `products` = conteo de distintos necesita guardar el conjunto de ASIN por grupo) |

Medir cada técnica con el instrumento actual, **1 repetición** por nivel.

**Hecho cuando:** hay una tabla técnica × nivel con "terminó / falló / tiempo" en `docs/evidencia/` y un párrafo de por qué ninguna resulta práctica.

## T4 — Medir la versión Spark optimizada (media)

Cuando Cristhian avise que `spark_version/run.py` está en `main`:

```powershell
.venv\Scripts\python -m spark_version.run --help
```

Correr la escalera con la versión optimizada (3 repeticiones en 1 % y 10 %, 1 en 50 % y 100 %) y agregar sus filas al CSV con `engine = spark_optimized`.

**Hecho cuando:** el CSV tiene filas `spark_optimized` y el reporte las compara con `spark` sin optimizar.

## Cómo entregar

1. Una rama nueva desde `main`: `f7-volumen-texto`.
2. Un PR hacia `main` con la plantilla de relevo. Pon en el título qué tareas incluye, por ejemplo `T1 + T2: volumen con texto y evidencia de fallo de pandas`.
3. No subas datos (`data/` y `output/` siguen ignorados). Las capturas y los CSV de evidencia **sí** se suben.
4. Avisa por WhatsApp cuando el PR esté listo: Cristhian hace una verificación rápida (la revisión rotativa que exige el plan y la sustentación) y lo fusiona.

## Especificación compacta (para agentes)

```yaml
owner: daniel (github: nosee8)
base_branch: main
work_branch: f7-volumen-texto
deadline: imminent; deliver partial work if needed, documented in the PR
do_not_edit:
  - spark_version/optimized.py
  - spark_version/run.py
  - tests/test_spark_optimized.py
  - docs/informe/**
  - docs/graficos/**
stable_api: spark_version/analysis.py function names and signatures must not change
tasks:
  - id: T0
    priority: now
    goal: share data/samples/ladder/catalog.parquet and the 1% ladder sample via Drive
  - id: T1
    priority: critical
    timebox_hours: 3
    goal: facts from raw/review_categories/Home_and_Kitchen.jsonl (31.41 GB) with text; pandas must fail on a 15.3 GB RAM machine
    steps:
      - add REVIEW_JSONL_URL to benchmark/config.py; give sources.py resume support first (today it deletes the .part and restarts from zero) or download in Range windows with fetch_range
      - convert once with Spark using an explicit schema (no inference) to Parquet
      - add text_length (chars of text, null -> 0) in both engines; add avg_text_length (4 decimals) to both output tables
      - update contract sections 2 and 6 and the equivalence tests
      - ladder 1/10/50/100 %; 3 repeats at 1 and 10 % for both engines and for pandas at 50/100 %; 1 repeat for spark at 50/100 % (documented in the report)
      - naive attempt pd.read_json(full_jsonl, lines=True) under the instrument as a subprocess with a 15-20 min timeout; record the exact error
    done_when:
      - at least one pandas run recorded as failed/MemoryError/killed or system RAM >= 95 %
      - spark finishes 100 % and writes both Parquet tables
      - outputs equivalent where both finish (contract section 8)
      - pytest -q passes
  - id: T2
    priority: critical
    goal: 5 Task Manager / Spark UI screenshots in docs/evidencia/capturas/
  - id: T3
    priority: high
    timebox_hours: 1.5
    goal: pandas mitigations (column selection, dtypes, chunks) in pandas_version/mitigated.py; table technique x level
  - id: T4
    priority: medium
    goal: measure spark_version.run (optimized) and add rows with engine=spark_optimized
deliver: PR to main using the handoff template; evidence CSV and screenshots committed, data never committed
