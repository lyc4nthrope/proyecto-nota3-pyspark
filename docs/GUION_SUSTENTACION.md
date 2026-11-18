# Guion de sustentación — Proyecto Nota 3

**Duración objetivo:** 15 min de exposición, más preguntas.
**Expositores:** Cristhian (C) y Daniel (D).
**Regla del enunciado:** *cada integrante debe poder explicar cualquier parte*. Los dos deben leer el guion completo, no solo su parte.

## 0. Antes de empezar (checklist, 10 min antes)

- [ ] Equipo Windows de Daniel con el repo en `main` y `.venv` activo.
- [ ] `.venv\Scripts\python -m pytest -q` en verde (prueba rápida de que Java, Spark y Parquet funcionan).
- [ ] Abiertos en pestañas:
  - `docs/informe/es/informe.pdf`
  - `docs/graficos/tiempo_total.png`
  - `docs/graficos/memoria_pico.png`
  - `docs/evidencia/capturas/`
  - el repo en GitHub
- [ ] Muestra del 1 % localizada: `dir data\samples\ladder_raw`.
- [ ] Administrador de tareas abierto en la pestaña **Rendimiento → Memoria**.
- [ ] Cerrar navegadores y programas pesados; si no, la RAM del equipo distorsiona la demo.

## 1. Estructura (15 min)

| Min | Bloque | Quién | Punto del enunciado |
|---|---|---|---|
| 0–1 | Apertura y tesis | C | Objetivo general |
| 1–3 | Problema y datos | C | §6.1 Introducción |
| 3–5 | Teoría mínima | C | §6.2 Marco teórico |
| 5–6 | Entorno Windows | D | §6.3 Entorno |
| 6–9 | Código de ambas versiones | C (Spark) + D (pandas) | §6.4 Código, 25 % de la rúbrica |
| 9–11 | Evidencia de inviabilidad | D | §6.5, 30 % de la rúbrica |
| 11–13 | Comparativa | C | §6.6 |
| 13–14 | Demo en vivo | D | Entregable 5.6 |
| 14–15 | Conclusiones | C | §6.7 |

## 2. Guion por bloque

### 0–1 · Apertura (C)

> "Queremos demostrar con mediciones que procesar 67 millones de reseñas de Amazon **con su texto** no es viable con pandas en un equipo de 15,3 GB, y que Spark sí lo resuelve en unos 3,6 minutos con menos de 6 GB."

Muestra la portada: integrantes y docente.

### 1–3 · Problema y datos (C) — §6.1

- **Dominio:** comercio electrónico.
- **Dataset:** *Amazon Reviews 2023* (McAuley Lab, UCSD), categoría Home and Kitchen.
- **Preguntas de negocio:**
  1. Evolución de las reseñas por año, mes y día de la semana.
  2. Tiendas con más volumen.
  3. Calificación media y su dispersión por grupo.
  4. Registros incompletos.
  5. Reseñas sin producto en el catálogo.
- **Datos** (tabla 1 del informe):
  - reseñas completas en JSONL: **31,4 GB**, 67.409.944 filas;
  - catálogo: 11,8 GB, 3,7 M productos.
- **Por qué es Big Data:**
  - **Volumen:** el archivo duplica la RAM del equipo.
  - **Variedad:** datos tabulares más texto libre.
  - **Memoria y no disco:** lo que importa es cuánto ocupa cargado en memoria, no en disco.
- **Frase clave:** "Al principio usamos solo las calificaciones (3,86 GB), y pandas sí terminaba. Por honestidad cambiamos al archivo con texto, que es el análisis real; ahí pandas deja de ser viable."

### 3–5 · Teoría mínima (C) — §6.2

Explícalo con el plan físico del informe (Fragmento 6 de la sección 4.8):

- **Driver:** construye el plan y reparte las tareas.
- **Executors:** ejecutan las tareas sobre las particiones. En modo `local[*]` son hilos del mismo equipo.
- **Evaluación perezosa:** `filter`, `join` y `groupBy` solo anotan el plan. Nada corre hasta una **acción** (`count`, `write`).
- **DAG y etapas:** cada **shuffle** (`Exchange`) corta el plan en etapas.
- **Particiones:** son la unidad de paralelismo y la razón de que Spark no necesite todo en memoria.
- **pandas:** un solo nodo, todo en memoria y ejecución inmediata (eager). Por eso su memoria crece con los datos.

### 5–6 · Entorno Windows (D) — §6.3

- **Equipo:** Windows 11, 8 núcleos lógicos, 15,3 GB de RAM.
- **Software:** Python 3.13, OpenJDK 17, PySpark 4.3.0, pandas 3.1.1. Todo fijado en `requirements.txt`.
- **Variables:**
  - `JAVA_HOME` la crea `winget`;
  - `HADOOP_HOME` y `winutils.exe` + `hadoop.dll` los instala `scripts/setup_winutils.ps1`, que además verifica el SHA-256;
  - `SPARK_HOME` no hace falta, porque pip trae Spark.
- **Dificultades** (tabla 3; cuenta 2):
  1. Faltaba `hadoop.dll` y escribir Parquet daba `UnsatisfiedLinkError`.
  2. La descarga de 31 GB no se podía reanudar. Ahora se conserva el `.part` y se continúa con `Range`.

### 6–9 · Código (C + D) — §6.4 (25 %)

**No lean el código línea por línea**: la rúbrica castiga describir sin justificar. Por cada paso, digan **qué decisión tomaron y por qué**.

| Paso | pandas (D) | Spark (C) | Concepto a nombrar |
|---|---|---|---|
| Lectura | `read_parquet` | `spark.read.schema(...)` con **esquema explícito** | Inferir el esquema de 31 GB obligaría a leerlo dos veces |
| Limpieza | máscaras booleanas | `filter` | Transformación perezosa |
| Derivadas | `.dt` | `F.year`, `F.dayofweek`, `text_length` | Mismo resultado: hay pruebas de equivalencia |
| Join | `merge(how="left")` | `join(how="left")` | Shuffle, o broadcast en la optimizada |
| Agregación | `groupby().agg()` | `groupBy().agg()` | Agregación parcial por partición y luego combinación |
| Escritura | un Parquet | carpeta con un archivo por partición | **La escritura es la acción que dispara todo** |

**Versión optimizada (C), tabla 4 del informe:**
1. Leer solo 4 columnas más `text`: Parquet es columnar.
2. Tabla temporal **sin join**: un shuffle menos en el DAG.
3. **Broadcast join** del catálogo (2 columnas). Mostrar `BroadcastHashJoin` en el plan.
4. Particiones de shuffle: 2 por núcleo en lugar de 200.
5. `coalesce(1)`, porque las tablas de salida son pequeñas.
6. Caché **apagada**: la medimos y salió más lenta.

> **Mensaje:** "La optimizada no cambia ninguna transformación, solo **cómo** se ejecuta. Por eso da las mismas tablas, y hay 46 pruebas que lo verifican."

### 9–11 · Evidencia de inviabilidad (D) — §6.5 (30 %)

1. **Método:**
   - cada corrida en su propio proceso;
   - una guardia de RAM mata el árbol de procesos (JVM incluida) cerca del 98 %;
   - enfriamiento entre corridas;
   - se registra la memoria **privada**, porque Windows recorta la residente.
2. **Intento ingenuo:** `pd.read_json` de los 31 GB comprometió **30.305 MB en 20 s** y fue cortado al 98 %. Muestra las figuras 2 y 3.
3. **Escalera** (tabla 5):
   - pandas termina el 1 % y el 10 %;
   - **al 50 % y al 100 % lo cortaron 3 de 3 veces**;
   - Spark termina los 4 niveles con un pico de 5.897 MB.
   - Muestra la figura de memoria.
4. **Mitigaciones** (tabla 6). El enunciado exige intentarlas:
   - **solo columnas** y **tipos compactos:** mueren al 50 % y al 100 %, porque el texto es parte del análisis y no se puede quitar;
   - **chunks:** termina, pero en **57,7 min contra 3,6 de Spark** (16 veces más lento) y con 194 líneas de lógica reescrita a mano.
   - **Frase:** "pandas es inviable **aun mitigado**."

### 11–13 · Comparativa (C) — §6.6

1. **Cómo se mide** (sección 6.1). Explícalo, porque demuestra que entienden la evaluación perezosa:
   - el benchmark mide cada paso con `count()` sin caché, así que cada paso **re-ejecuta todo** desde la lectura;
   - sumar los pasos daba 705,7 s;
   - una pasada real (`write_parquet`) da **214,6 s**.
2. **Tabla 7:**

   | Muestra | pandas | Spark |
   |---|---|---|
   | 1 % | 4,7 s | 12,2 s |
   | 10 % | 27,8 s | 32,3 s |
   | 50 % | cortado | 161,3 s |
   | 100 % | cortado | 214,6 s |

3. **Interpretación crítica:**
   - con datos pequeños gana pandas (2,6 veces más rápido), porque Spark paga el arranque de la JVM y la planificación;
   - la brecha se cierra al crecer;
   - con datos grandes **solo Spark termina**.
4. **Optimizada:** 123,4 s contra 214,6 al 100 % (1,74 veces más rápida), pero **más lenta al 10 %**: las optimizaciones tienen un umbral a partir del cual compensan.
5. **Complejidad** (tabla 8): pandas 86 líneas, Spark 108, la optimizada 76 y los chunks 194. Leer más allá de la memoria con pandas obliga a reimplementar lo que Spark ya hace.

### 13–14 · Demo en vivo (D)

Usen la muestra del **1 %**, que tarda segundos. **Nunca** el 100 % en vivo.

```powershell
.venv\Scripts\python -m spark_version.run `
  --reviews data\samples\ladder_raw\<muestra_1pct>.parquet `
  --catalog data\samples\ladder\catalog.parquet `
  --output output\demo --explain output\demo\plan.txt
```

1. Mientras corre, abran `http://localhost:4040` (Spark UI): **Jobs → Stages**. Señalen las etapas cortadas por el shuffle.
2. Muestren la línea JSON del resultado (filas, tiendas, segundos).
3. Abran `output\demo\plan.txt` y busquen `BroadcastHashJoin`.
4. Recuerden que la UI de Spark se cierra cuando termina el programa. Si no alcanza a mostrarse, usen la captura `spark_ui_dag.png`.

**Plan B si la demo falla:**
- mostrar `pytest -q` en verde;
- mostrar las capturas de `docs/evidencia/capturas/`;
- decir: "las cifras salen de `docs/evidencia/*.csv`, y el README dice cómo reproducir cada una".

### 14–15 · Conclusiones (C) — §6.7

1. La hipótesis se cumple: pandas no puede procesar los datos desde el 50 %, ni siquiera con mitigaciones; Spark termina el 100 %.
2. Spark **no** es mejor por defecto: con datos pequeños pierde. Es la herramienta correcta cuando los datos ya no caben en memoria.
3. La evaluación perezosa importa incluso al **medir**.
4. **Casos reales:** el patrón "tabla grande de eventos + catálogo pequeño + agregación" es la base de la analítica de comercio electrónico, la recomendación y los reportes. A escala de un gran minorista se lleva a un clúster cambiando solo el *master*.
5. **Limitaciones (díganlas ustedes antes de que se las pregunten):**
   - Spark se midió 1 vez al 50 % y al 100 %, por el tiempo de cada corrida;
   - no se midió la CPU;
   - todo es modo local, sin clúster;
   - el tiempo de una pasada de la base es una estimación.

## 3. Cifras para memorizar

| Dato | Valor |
|---|---|
| Reseñas con texto | 67.409.944 (31,4 GB en JSONL, 9,72 GB en Parquet) |
| RAM del equipo | 15,3 GB |
| Intento ingenuo de pandas | 30.305 MB en 20,1 s, cortado al 98 % |
| pandas al 50 % / 100 % | cortado 3 de 3 veces (97–99 % de RAM) |
| Spark al 100 % | 214,6 s en una pasada; pico de 5.897 MB |
| Spark optimizado al 100 % | 123,4 s (1,74 veces más rápido) |
| pandas con chunks al 100 % | 3.460 s = 57,7 min (unas 16 veces más lento) |
| Equivalencia | 9/9 pares idénticos al 1 % y al 10 %; 0 reseñas sin catálogo |
| Pruebas | 46 automáticas |

## 4. Preguntas probables del docente (con respuesta)

### Sobre el problema y los datos

1. **¿Por qué es Big Data y no "un archivo grande"?**
   - Por volumen: con el texto en memoria, el archivo supera la RAM.
   - Por variedad: datos tabulares más texto libre.
   - Y porque lo demostramos midiendo: pandas falla. No es una suposición.
2. **¿Por qué cambiaron del CSV al JSONL?**
   - Con el CSV de 3,86 GB pandas terminaba el 100 %, así que el volumen no justificaba Spark.
   - El análisis real usa el texto (`avg_text_length`), y con el texto pandas deja de ser viable.
   - Preferimos mostrarlo con honestidad.
3. **¿El dataset se cita?** Sí: la referencia [1] (Hou et al., 2024) y Hugging Face. Se descarga por URL, sin ejecutar código ajeno.

### Sobre Spark (teoría)

4. **¿Qué es la evaluación perezosa y dónde se ve en su código?**
   - Las transformaciones solo construyen el plan; lo ejecuta la acción.
   - En nuestro código, `write_outputs` es la acción que dispara todo.
   - Se ve también en la medición: sumar los pasos contaba varias veces el mismo trabajo.
5. **¿Qué es un shuffle y cuántos tiene su plan?**
   - Es redistribuir datos entre particiones por clave; aparece como `Exchange`.
   - En la base: el join más cada agregación.
   - En la optimizada: el broadcast elimina el shuffle del join y la tabla temporal no tiene join.
6. **¿Por qué el broadcast join funciona aquí?**
   - El catálogo reducido a `parent_asin` + `store` es pequeño.
   - Se copia a cada executor y las 67 M reseñas se unen donde están, sin moverse.
   - Si el catálogo no cupiera en memoria, el broadcast fallaría y habría que usar sort-merge.
7. **¿Driver y executors en modo local?** En `local[*]` el driver y los executors están en la misma JVM, y los executors son hilos, uno por núcleo. El modelo es el mismo que en un clúster.
8. **¿Por qué no usaron caché?**
   - La medimos: con 20 M filas fue más lenta (28,8 s frente a 25,7 s).
   - Releer 4 columnas Parquet cuesta menos que guardar todo en memoria.
   - Está disponible con `--cache`.
9. **¿Por qué 2 particiones de shuffle por núcleo?** Las 200 por defecto crean muchas tareas diminutas en modo local, y cada tarea tiene un costo fijo.
10. **¿Usaron `collect()` o `toPandas()`?** No sobre los datos. Solo leemos las tablas agregadas, que son pequeñas. La rúbrica castiga lo contrario.

### Sobre la medición (lo más probable)

11. **¿Por qué al 1 % pandas es más rápido? ¿No contradice su tesis?**
    - No: la tesis es sobre volumen.
    - Con pocos datos, Spark paga el arranque de la JVM, la planificación y el scheduling, que no se amortizan.
    - Lo decimos como limitación de Spark, que el propio enunciado pide.
12. **¿Cómo saben que pandas "falló" y no que lo mataron ustedes?**
    - Lo cortó la guardia al 97–99 % de RAM, antes de que el sistema se congelara.
    - El registro guarda la memoria privada al momento del corte: 12–14,5 GB, casi toda la RAM del equipo.
    - El intento ingenuo llegó a 30 GB comprometidos en 20 s.
    - El enunciado acepta como evidencia "RAM cercana al 100 %".
13. **¿Por qué solo 1 repetición de Spark al 50 % y al 100 %?** Cada corrida tarda varios minutos y el plazo no alcanzó. pandas tiene sus 3. Lo declaramos como limitación.
14. **¿Por qué no midieron la CPU?** El enunciado exige como mínimo tiempo y memoria. La CPU quedó fuera por tiempo, y también lo declaramos.
15. **¿Cómo garantizan que ambas versiones dan el mismo resultado?**
    - Con pruebas automáticas de equivalencia (`tests/test_analysis_equivalence.py`).
    - Comparando las tablas al 1 % y al 10 %: 9 de 9 pares idénticos.
16. **¿Mismo equipo y misma configuración?** Sí: todo en el equipo Windows de Daniel, con las versiones fijadas (tabla 2 del informe).

### Sobre pandas y las mitigaciones

17. **¿Probaron chunks? Con chunks pandas sí suma por grupos.**
    - Sí, y termina el 100 %, pero en 57,7 min contra 3,6.
    - Además hay que mantener 3,7 M ASIN en conjuntos para contar productos distintos, reescribir la desviación con sumas de cuadrados, y no se pueden hacer ordenamientos ni percentiles globales sin releer todo.
    - Funciona, pero no es práctico.
18. **¿Por qué no quitaron el texto para que pandas cupiera?** Porque es parte del contrato del análisis (`avg_text_length`). Quitarlo cambia la pregunta de negocio.

### Sobre el código y la reproducibilidad

19. **¿Cómo ejecuto el proyecto en mi Windows?** Con el README:
    1. `winget` para Python 3.13 y JDK 17;
    2. `scripts/setup_winutils.ps1`;
    3. `pip install -r requirements.txt`;
    4. `pytest -q`.

    La sección "Reproducir los resultados del informe" del README tiene un comando por cada cifra.
20. **¿Por qué esquema explícito en Spark?** Inferir el esquema de 31 GB de JSON obliga a leerlo completo una vez más, y los tipos inferidos pueden variar entre archivos.
21. **¿Por qué la optimizada es más lenta al 10 %?** El broadcast y la poda tienen un costo fijo que no se paga con pocos millones de filas. Al 100 % gana 1,74 veces. Es el umbral a partir del cual la optimización compensa.

### Preguntas "trampa" para cada integrante

- **A Daniel (sobre Spark):** "Explique este `BroadcastHashJoin` del plan." Respuesta: la pregunta 6.
- **A Cristhian (sobre pandas):** "¿Por qué la memoria de pandas crece con los datos?" Respuesta: carga todo el DataFrame a la vez, y el texto son objetos de Python, que ocupan mucho más en memoria que en disco.
- **Sobre Maria:** está en la portada como integrante del grupo; el trabajo lo desarrollaron Cristhian y Daniel. Respondan con hechos y sin juicios.
