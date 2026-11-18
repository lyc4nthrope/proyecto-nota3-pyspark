# Bitácora didáctica — Proyecto Nota 3

Registro cronológico de **acciones, comandos y librerías**, explicando para qué
sirve cada cosa y cómo ayuda. Su destino es el documento final de lo realizado:
cada entrada debe poder leerse meses después y entenderse sin contexto.

Diferencia con `BITACORA.md`: ese registroa las **dificultades y cómo se
resolvieron** (alimenta la sección de entorno del informe); este registra **qué
se hizo y con qué herramientas** (alimenta el documento de lo realizado).

### Plantilla de entrada

```
### AAAA-MM-DD — Título corto
**Qué se hizo.** ...
**Por qué.** ...
**Comandos.** ...
**Librerías y herramientas.** ... (para qué sirve y cómo ayudó aquí)
**Resultado.** ... (evidencia observable)
```

---

## Glosario de librerías y herramientas

| Herramienta | Para qué sirve | Cómo ayuda en este proyecto |
|---|---|---|
| **pandas** | DataFrames en memoria, operaciones tipo hoja de cálculo | Es la versión de referencia: correcta, simple, y la que debe dejar de ser viable con datos grandes |
| **PySpark** (`pyspark.sql`) | Motor distribuido de DataFrames sobre la JVM; usa disco cuando no cabe en RAM | Es la versión que debe terminar donde pandas no puede; aquí corre en modo `local[*]` |
| **pyarrow** | Implementación de Parquet que pandas usa por debajo | Lee y escribe el formato columnar comprimido que comparten ambas versiones |
| **psutil** | Inspección de procesos del sistema operativo | Mide la memoria residente pico del árbol de procesos completo (Python + JVM) |
| **pytest** | Framework de pruebas automatizadas | Ejecuta las pruebas de humo, de sintaxis y de equivalencia pandas ↔ Spark |
| **numpy** | Cálculo numérico de bajo nivel | En las pruebas, comparación de floats tolerante a NaN (un grupo de una fila no tiene desviación) |
| **urllib.request** (stdlib) | Cliente HTTP | Descarga por rangos (`Range: bytes=...`) para muestrear archivos gigantes sin bajarlos completos |
| **argparse** (stdlib) | Interfaz de línea de comandos | Convierte cada herramienta en un comando reproducible con opciones documentadas |
| **threading** (stdlib) | Hilos de ejecución | El muestreador de memoria corre en un hilo aparte mientras la operación corre en el principal |
| **json / csv** (stdlib) | Serialización | Manifiestos de las muestras (qué se descargó y con qué versión) y tablas de mediciones |
| **time.perf_counter** (stdlib) | Cronómetro de alta resolución | El tiempo de pared de cada operación medida |
| **xxhash64 / pmod** (SQL de Spark) | Hash determinista y resto positivo | Selecciona fracciones de filas reproducibles (1/10/50/100 %) sin ordenar ni descargar nada más |
| **subprocess** (stdlib) | Lanzar procesos hijos | Cada corrida de medición corre en proceso nuevo, para que la memoria medida sea solo de esa corrida |
| **PowerShell** | Shell de Windows | Instalación del entorno y ejecución de los comandos |
| **git / GitHub** | Control de versiones | Ramas por fase y revisión por pull request según el acuerdo del equipo |

---

## Entradas

### 2026-11-17 — Evaluación de datasets en Hugging Face

**Qué se hizo.** Se compararon candidatos del dataset `McAuley-Lab/Amazon-Reviews-2023`
(`All_Beauty`, `Home_and_Kitchen`, `Electronics`): tamaños, número de filas y
comportamiento de descarga.

**Por qué.** El proyecto necesita datos que superen la RAM del equipo de prueba
o que hagan fallar a pandas, y hay que elegir con números, no por intuición.

**Comandos.**
```powershell
# Tamaños y filas por categoría (API de Hugging Face)
Invoke-RestMethod "https://datasets-server.huggingface.co/size?dataset=McAuley-Lab/Amazon-Reviews-2023"
# ¿El servidor acepta rangos de bytes? (clave para muestrear sin descargar)
curl.exe -sIL "https://huggingface.co/datasets/.../resolve/main/raw/review_categories/Home_and_Kitchen.jsonl"
# Traer solo un tramo del archivo
curl.exe -sL -r 0-5000000 -o muestra.jsonl <url>
```

**Librerías y herramientas.** `Invoke-RestMethod` (consultas HTTP desde
PowerShell) y `curl.exe` (descargas y cabeceras). La cabecera
`Accept-Ranges: bytes` es la que permite pedir trozos de un archivo gigante:
eso convirtió "hay que bajar 31 GB" en "se puede muestrear con unos MB".

**Resultado.** `Home_and_Kitchen` es la categoría más grande: 31,4 GB de reseñas
crudas (67,4 M filas) o 3,86 GB en la tabla publicada de interacciones. Con
todas las columnas pandas ocuparía ~45 GB; incluso con selección de columnas
~16 GB, por encima de los 15,3 GB de RAM del equipo.

### 2026-11-17 — Revisión del arnés de medición existente

**Qué se hizo.** Se leyó `benchmark/measure_sample.py` (sonda de 1.150 líneas)
y se comparó contra lo que la evidencia necesita.

**Por qué.** Antes de escribir código hay que saber qué mide lo que ya existe y
qué huecos tiene.

**Comandos.**
```powershell
.venv\Scripts\python -m benchmark.measure_sample --help
.venv\Scripts\python -m pytest -q
```

**Resultado.** La sonda medía bien las operaciones Spark, pero no existía la
contraparte pandas, ni la escalera de muestras (1/10/50/100 %), ni las
repeticiones, y la tasa de filas sin catálogo salía 0 % por construcción de la
muestra.

### 2026-11-17 — Reestructuración del arnés por responsabilidades

**Qué se hizo.** Las 1.150 líneas se dividieron en módulos pequeños
(`config`, `sources`, `measure`, `profile`, `runner`, `report`, CLI) y las
transformaciones del análisis se movieron a `spark_version/analysis.py`.

**Por qué.** Lo medido debe ser lo entregado: si las transformaciones viven en
el pipeline y el medidor solo las ejecuta, se evita duplicar código y que las
mediciones describan un programa distinto al que se entrega.

**Comandos.**
```powershell
.venv\Scripts\python -m pytest -q          # antes y después: 9 pruebas verdes
.venv\Scripts\python -m benchmark.measure_sample   # corrida diferencial
```

**Resultado.** Las 10 operaciones produjeron resultados idénticos a la corrida
anterior (verificación diferencial contra el `run.json` guardado). Las claves de
caché de los manifiestos se conservaron para no volver a descargar 3,7 GB.

### 2026-11-17 — Contraparte pandas y escalera de comparación

**Qué se hizo.** Se creó `pandas_version/analysis.py` (espejo exacto de las
transformaciones de Spark), `benchmark/ladder.py` (muestras por fracción de
filas y catálogo compartido), `benchmark/pandas_runner.py` y
`benchmark/compare.py` (orquestador).

**Por qué.** Sin la contraparte pandas no hay comparación posible, y sin una
escalera de muestras no se puede mostrar dónde deja de ser viable.

**Comandos.**
```powershell
.venv\Scripts\python -m pytest tests/test_analysis_equivalence.py -q
.venv\Scripts\python -m benchmark.compare --build-only --levels 0.01
.venv\Scripts\python -m benchmark.compare --single-run --engine pandas --level 0.01 --rep 1
```

**Librerías y herramientas.** `xxhash64` + `pmod` de Spark (selección
determinista de filas: la misma muestra en cualquier máquina),
`subprocess` (un proceso nuevo por corrida, porque pandas y la JVM retienen
memoria liberada), `pytest` + `numpy` (equivalencia entre motores, incluida la
numeración de días de Spark donde 1=domingo).

**Resultado.** 7 pruebas de equivalencia verdes: mismas filas descartadas,
mismo join, mismos agregados, misma numeración de días.

### 2026-11-17 — Catálogo completo y primeras mediciones

**Qué se hizo.** Se bajó el metadata completo (11,788 GB), se proyectó a un
Parquet con las 6 columnas del análisis (3.735.584 productos) y se construyó el
nivel 1 % de la escalera (665.617 de 66.623.880 filas, 0,9991 %).

**Por qué.** El join debe costar lo mismo en ambos motores y en todos los
niveles: ambos leen el mismo catálogo proyectado.

**Comandos.**
```powershell
.venv\Scripts\python -m benchmark.compare --build-only --levels 0.01
.venv\Scripts\python -m benchmark.compare --single-run --engine spark --level 0.01 --rep 1
```

**Resultado (nivel 1 %).** pandas: ~7 s en total y 1,1 GB de memoria pico en el
join. Spark: ~93 s (tiempos acumulativos) y 2,9 GB de pico. En volúmenes
pequeños pandas gana; la escalera mostrará el punto de inversión.

### 2026-11-17 — Correcciones encontradas al verificar

**Qué se hizo.** Se corrigieron tres defectos detectados por la verificación,
no por suposición:

1. Spark escribe `agg_by_store.parquet` como **directorio** de partes y pandas
   como **archivo**: al compartir ruta, pandas fallaba con `WinError 5`. Salidas
   separadas por motor (`tables/spark/`, `tables/pandas/`).
2. La lectura manual de partes de Parquet duplicaba el pico de memoria justo en
   los tamaños que se miden; se delegó a `pd.read_parquet(dir)`, que ignora los
   archivos de control de Spark.
3. La comparación de desviaciones fallaba porque un grupo de una fila produce
   `NaN` en ambos motores y `NaN != NaN`; se comparó de forma tolerante a NaN.

**Por qué.** Un medidor que falla mide mal; cada corrección fue observada en una
corrida real, con su error y su arreglo.

### 2026-11-17 — Corrida completa de la escalera (en curso)

**Qué se hizo.** Se lanzó la comparación completa: 2 motores × 4 niveles
(1/10/50/100 %) × 3 repeticiones = 24 corridas aisladas, con timeout de 1 h por
corrida.

**Comandos.**
```powershell
.venv\Scripts\python -m benchmark.compare
```

**Resultado.** Pendiente: se registrará la tabla comparativa, el punto de
quiebre de pandas y la variación entre repeticiones.

### 2026-11-17 — Calidad medida de la fuente: duplicados y descartes

**Qué se hizo.** Se midió, sobre la muestra del 1 % (665.617 filas), cuántos
duplicados hay y cuántas filas descarta cada regla de limpieza.

**Por qué.** El contrato no puede fijar la regla de duplicados sin saber si
existen, y la evidencia de limpieza necesita el desglose por regla, no un total.

**Comandos.**
```powershell
.venv\Scripts\python -c "import pandas as pd; df = pd.read_parquet('data/samples/ladder/reviews_1pct.parquet'); ..."
```

**Librerías y herramientas.** `pandas.read_parquet` (lectura de la muestra) y
`DataFrame.duplicated` / `dropna` / filtros por rango (los tres tipos de
diagnóstico de calidad).

**Resultado.** **Cero duplicados y cero descartes**: la tabla 0core viene
deduplicada y filtrada en el origen. Las reglas de limpieza quedan como
validación sobre esta fuente y siguen siendo necesarias para el JSONL crudo.
Los números se copiaron al contrato.

### 2026-11-17 — Primera versión del análisis (pandas)

**Qué se hizo.** Se creó `pandas_version/pipeline.py`: lee el origen (Parquet
de muestra o CSV completo), aplica las reglas del contrato, calcula las
columnas derivadas, hace el join, agrega y escribe las dos tablas de salida
más un `summary.json` con los conteos de cada etapa. Se añadió una prueba que
fija esos conteos y las invariantes.

**Por qué.** La primera versión es el oráculo: sin ella no se puede decir que
la versión Spark está bien. Se construye contra el contrato, no contra las
mediciones, y sin optimizaciones a propósito.

**Comandos.**
```powershell
.venv\Scripts\python -m pytest tests/test_pandas_pipeline.py -q
# (tras la corrida) referencia sobre la muestra del 1 %:
.venv\Scripts\python -m pandas_version.pipeline
```

**Resultado.** Prueba verde: conteos por etapa correctos, las tres reglas de
descarte suman exactamente las filas perdidas, el join izquierdo no cambia el
número de filas y las salidas tienen las columnas del contrato. La corrida de
referencia sobre la muestra queda pendiente hasta terminar la medición, para
no distorsionar los tiempos.

### 2026-11-18 — Resultados de la escalera y validación lógica

**Qué se hizo.** Terminó la corrida completa (24/24 sin errores) y se validó
por análisis lógico sobre los datos crudos de `run_ladder.json`.

**Comandos.**
```powershell
.venv\Scripts\python -m pandas_version.pipeline   # referencia sobre el 1 %
```

**Resultado — tiempos totales medios y memoria pico máxima (3 repeticiones):**

| Nivel | Filas | pandas tiempo | Spark tiempo | pandas pico | Spark pico |
|---|---|---|---|---|---|
| 1 % | 665.617 | 4,9 s | 72,9 s | 1,2 GB | 3,2 GB |
| 10 % | 6.664.148 | 25,7 s | 121,3 s | 2,8 GB | 5,0 GB |
| 50 % | 33.313.034 | 143,1 s | 241,1 s | 8,1 GB | 6,6 GB |
| 100 % | 66.623.880 | 205,2 s | 311,1 s | 10,2 GB | 8,2 GB |

**Validación lógica.** 12/12 pares motor×nivel con resultados idénticos en
las 6 operaciones de análisis (lectura, limpieza, derivadas, join y los dos
agregados). La única diferencia es física: Spark escribe entre 5 y 9 archivos
*parte* por tabla y pandas 2 archivos sueltos. Invariantes verificadas: 0
descartes en los cuatro niveles (coherente con la medición de calidad), el
join conserva las filas y 0 sin catálogo, los grupos crecen de forma
monótona (84.436 → 375.506 tiendas; 1.704 → 1.985 grupos temporales) y los
conteos escalan linealmente con la muestra.

**Lectura crítica.** La tendencia es la esperada —la ventaja de tiempo de
pandas cae de 15x a 1,5x y Spark ya usa menos memoria desde el 50 %—, pero
**pandas termina el 100 %**: con esta tabla, el volumen no justifica Spark, que
es justo lo que la rúbrica castiga. El marco de pandas al 100 % ocupa 12,2 GB
en un equipo de 15,3 GB, así que está al límite, pero no falla. Decisión
pendiente sobre cómo asegurar el volumen (punto 5 del contrato).

### 2026-11-18 — Escalera con texto: pandas no termina (evidencia de T1)

**Qué se hizo.** Ruta completa con el JSONL crudo (31,41 GB, 67,4 M reseñas
con texto): descarga reanudable, conversión única a Parquet con esquema
explícito (9,72 GB), `text_length`/`avg_text_length` en ambos motores y
escalera 1/10/50/100 % con la política de repeticiones corregida.

**Resultado (medias de 3 repeticiones; Spark al 50/100 % con 1, según lo
acordado por plazo):**

| Nivel | Filas | pandas | Spark |
|---|---|---|---|
| 1 % | 673.379 | 4,7 s · 1,3 GB — termina | 52,7 s · 3,8 GB — termina |
| 10 % | 6.742.408 | 27,8 s · 4,7 GB — termina | 106,8 s · 4,2 GB — termina |
| 50 % | 33.706.117 | **matado al 97–98 % de RAM** (~13 GB privados al caer) | 433,3 s · 5,3 GB — termina |
| 100 % | 67.409.944 | **matado al 98–99 % de RAM** (~14 GB privados al caer) | 705,7 s · 5,9 GB — termina |

**Intento ingenuo:** `pd.read_json` sobre los 31 GB comprometió **30,3 GB de
memoria privada en 20 segundos** antes de que el sistema cortara al 98 % de
RAM. Ese es el error textual registrado que pedía la tarea.

**Validación lógica:** equivalencia **9/9 pares idénticos** entre motores al
1 % y 10 % (mismos grupos, mismo top-1), y en todas las corridas terminadas:
0 descartes por limpieza y 0 filas sin catálogo.

**Dos defectos del propio instrumento, encontrados y corregidos midiendo:**
las repeticiones arrancaban con el sistema todavía saturado (el guardia mataba
recién nacidas, con árboles de 1,9 MB) y el corte registraba el residente,
que Windows recorta a casi cero; ahora hay enfriamiento entre corridas y se
registran bytes privados. Se rehizo todo lo tocado.

**Conclusión para el informe:** con el texto en el análisis, pandas no es
viable en el equipo de 15,3 GB ni al 50 %; Spark termina el 100 % con ~6 GB
de pico. La comparación honesta de tiempos donde ambos terminan muestra a
pandas más rápido en pequeño (4,7 s vs 52,7 s al 1 %), con la ventaja
reduciéndose al crecer los datos.

### 2026-11-18 — T3: mitigaciones de pandas, ninguna basta

**Qué se hizo.** Las tres técnicas que exige el enunciado antes de declarar la
inviabilidad, cada una en su proceso medido, sobre los hechos con texto: selección
de columnas, tipos optimizados y lectura por *chunks* con agregación parcial.

| Técnica | 1 % | 10 % | 50 % | 100 % |
|---|---|---|---|---|
| Selección de columnas | 5,3 s | 25,8 s | matada al 99 % de RAM | matada al 98 % |
| Tipos optimizados | 7,3 s | 20,3 s | matada al 98 % | matada al 97 % |
| *Chunks* + agregación parcial | 67 s | 835,7 s | 2.401 s | **3.460 s (57,7 min)** |

**Lectura.** Las dos primeras terminan hasta el 10 % y mueren después: el
texto es columna del contrato (para `avg_text_length`), no se puede descartar
ni estrechar, y son sus bytes los que llenan la memoria. Los *chunks* son los
únicos que terminan el 100 %, pero 16 veces más lento que una pasada de Spark
(~3,6 min), reteniendo 3.734.414 ASINs vivos en conjuntos por tienda (~427 MB)
para calcular `products`, y sin poder expresar operaciones globales (ordenar
67 M de filas, percentiles, ventanas). El párrafo de conclusión queda en
`docs/evidencia/mitigaciones.md`.
