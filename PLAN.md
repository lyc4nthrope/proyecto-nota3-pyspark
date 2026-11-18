# Plan — Proyecto Nota 3: Big Data con Python y PySpark

Enunciado: `../clase11Nov2026/Proyecto Nota 3.pdf`. El enunciado dice grupos de 4; este grupo es de **3 personas, con aprobación del profesor** (confirmado 2026-11-12). Nota: informe 75 %, código 25 %.

## 1. Requisitos → estado actual

| Lo que pide el enunciado | Estado actual (evidencia) | Brecha |
|---|---|---|
| Caso de uso real de Big Data, justificado | No elegido | Elegir dominio, fuente y problema (D1) |
| Datos **mayores que la RAM** del equipo de prueba, o que hagan fallar a pandas | Este equipo: 15 GB de RAM, 16 núcleos, **11 GB libres en disco** (88 % usado) | Conseguir los datos; el disco es la restricción principal |
| Versión pandas + **evidencia de inviabilidad** (MemoryError, horas, RAM al 100 %) | No existe | Escribirla y medirla |
| Antes de declarar inviable: probar chunks, tipos optimizados y selección de columnas | No hecho | Implementar las 3 mitigaciones y documentar por qué no bastan |
| Versión PySpark equivalente: lectura, limpieza, transformaciones, **≥ 1 join**, agregaciones por grupo, escritura (Parquet) | No existe | Escribirla |
| ≥ 1 optimización: particionamiento, caching, Parquet, broadcast join | No existe | Aplicarla y medir su efecto |
| Escalabilidad con muestras de 1 %, 10 %, 50 %, 100 % | No hecho | Script de medición |
| Tiempo y memoria en cada corrida, **≥ 3 repeticiones** | No hecho | Script de medición |
| Mismo equipo y configuración para ambas versiones, con CPU, RAM, disco y versiones | No documentado | Registrar el entorno |
| **Windows 10/11**, JDK 8/11/17, `JAVA_HOME`, `SPARK_HOME`, `winutils.exe`; README para Windows | Este equipo es **Linux** (Debian). Java 17.0.18 y Python 3.13.5 instalados. **Dos integrantes usan Windows** (confirmado 2026-11-12) | Elegir cuál de los dos es el equipo de prueba y anotar su CPU, RAM y disco (D2) |
| Python ≥ 3.9 + PySpark | PySpark 4.3.0 exige Python ≥ 3.11 (PyPI, 2026-11-12) | Fijar versiones (D3) |
| Informe técnico PDF con 8 secciones | No existe | Escribirlo |
| Evidencias: capturas del Administrador de tareas, errores, registros de tiempo | No existen | Capturarlas en Windows |
| "Repositorio **o** carpeta comprimida" con ambos proyectos, `requirements.txt`, README y descripción de los datos | `context_bigData` **no es un repositorio git** | Repositorio no obligatorio, pero recomendado (D4) |
| Sustentación con demo en vivo; cada integrante explica cualquier parte | — | Ensayo |

### Qué califica (rúbrica)
1. **Explicación y análisis del código, 25 %**: justificar decisiones y relacionar el código con DAG, evaluación perezosa y particiones.
2. **Comparación y evidencia de inviabilidad, 30 %**: evidencia reproducible, mismo entorno, mediciones repetidas, tablas y gráficos, interpretación crítica.
3. **Fundamentación, conclusiones y calidad, 20 %**: marco teórico correcto, conclusiones basadas en resultados, limitaciones de Spark, referencias.
4. **Código, 25 %**: ambas versiones corren en Windows siguiendo el README, resultados equivalentes, buenas prácticas, código modular y documentado.

### Detalles a tener en cuenta
- Un 0–2,9 en código ocurre si "el volumen de datos no justifica el uso de Spark". El tamaño de los datos es lo primero que hay que asegurar.
- La rúbrica castiga el código Spark que "se limita a imitar a pandas" (por ejemplo `toPandas()` o `collect()` sobre todos los datos).
- La comparación debe ser honesta: con chunks, pandas sí puede sumar por grupos. Lo que hay que mostrar es dónde deja de ser práctico (join, ordenamientos y agregaciones globales, tiempo, complejidad del código).
- Los resultados de ambas versiones deben ser **equivalentes**: se comprueba en la muestra más grande que pandas sí termina.
- Los tamaños de datos de D1 son estimaciones: se miden en F1 antes de comprometerse.

## 2. Decisiones

| # | Decisión | Recomendación | Por qué |
|---|---|---|---|
| D1 | Caso y datos | **NYC TLC Yellow Taxi**, varios años, más la tabla `taxi_zone_lookup` (265 zonas) | Público, citado en el enunciado, tiene fecha, montos y zonas; la tabla de zonas da un **join real** y es el caso típico de broadcast join |
| D2 | Equipo de prueba | **Uno solo de los dos equipos Windows del grupo** para todas las mediciones: el de **menos RAM**, si tiene disco libre para los datos. El otro Windows hace la prueba limpia del README (F10). Desarrollo en cualquier equipo | El enunciado exige Windows y el mismo equipo para ambas versiones. Con menos RAM, pandas falla con menos datos: descarga más corta y evidencia más clara |
| D3 | Versiones | **JDK 17 + Python 3.11–3.13 + PySpark 4.x**, fijadas en `requirements.txt` | JDK 17 está en la lista del enunciado y es el que usa Spark 4; confirmar en F0 que arranca en Windows |
| D4 | Repositorio | **Sí: repositorio GitHub nuevo y propio** (`pandas_version/`, `spark_version/`, `benchmark/`, `docs/`), **sin datos** | Son 4 personas; el enunciado lo acepta; los datos van por script de descarga, como permite el punto 5.3 |
| D5 | Formato de origen | **El mismo conjunto de archivos para ambas versiones** | Comparación justa; se decide CSV o Parquet en F1 según el espacio en disco |
| D6 | Medición | Script único: tiempo con `time.perf_counter`, memoria pico con `psutil`, 3 repeticiones, salida a CSV | Lo exige el punto 4 y alimenta directamente las tablas y gráficos |
| D7 | Optimizaciones | **Parquet + broadcast join + particionado por fecha**; `cache` solo si un DataFrame se reutiliza | Cada una se puede medir antes y después, que es lo que pide el criterio 1 |
| D8 | Idioma | Informe en **español**; código y comentarios según indique el profesor | El enunciado está en español; confirmar si aplica la regla de inglés del Proyecto 1 |

## 3. Fases

Cada fase cabe en una sesión, termina verificada, con su propio commit, y se detiene en un punto de control.

| Fase | Qué incluye | Decisiones | Verificación | Estado |
|---|---|---|---|---|
| **F0** Entorno y repositorio | Crear el repositorio y la estructura; instalar JDK, Spark y winutils en el equipo Windows; anotar cada dificultad (va a la sección 3 del informe) | D2, D3, D4 | En Windows, `spark.range(10).count()` devuelve 10; `requirements.txt` instalable en limpio | 🟡 Entorno listo en Linux (P3, 2026-11-12) y en Windows (P1, 2026-11-15): `pytest -q` → **todas pasan** (6 en Windows). Falta que el revisor lo ejecute en su equipo |
| **F1** Datos | Script de descarga; descripción del conjunto y su origen; medir filas, tamaño en disco y tamaño estimado en RAM | D1, D5 | Tabla con filas y GB; el tamaño en RAM estimado **supera** la RAM del equipo de prueba | ⬜ |
| **F2** Definir el análisis | Escribir en una página las preguntas de negocio y las operaciones: limpieza, columnas derivadas, join con zonas, agregaciones, salida | D1 | Cada operación mínima del enunciado tiene al menos una pregunta que la usa | ⬜ Borrador de arranque en `docs/ANALISIS_BASE.md`, con **D1 todavía abierta** |
| **F3** pandas base | Versión directa sobre la muestra del 1 %; guardar el resultado de referencia | — | Corre completa en el 1 % y produce las tablas de F2 | ⬜ |
| **F4** pandas con mitigaciones | Chunks, tipos optimizados, selección de columnas | — | Las 3 técnicas están implementadas y se anota hasta qué muestra llega cada una | ⬜ |
| **F5** PySpark base | Versión equivalente con DataFrames o Spark SQL, sin `collect()` masivo | — | En el 1 %, el resultado **coincide** con el de pandas (misma cantidad de filas y mismos totales) | ⬜ |
| **F6** PySpark optimizado | Parquet, broadcast join, particionado; revisar `explain()` y la interfaz de Spark | D7 | El plan muestra `BroadcastHashJoin`; hay tiempo antes y después de cada optimización | ⬜ |
| **F7** Medición | Correr ambas versiones al 1 %, 10 %, 50 % y 100 %, 3 veces cada una, en el equipo Windows; capturas del Administrador de tareas y de los errores | D2, D6 | CSV con todas las corridas; evidencia del punto donde pandas falla; Spark termina el 100 % | ⬜ |
| **F8** Gráficos y análisis | Tabla y gráficos de tiempo, memoria y escalabilidad; comparación de líneas y legibilidad del código | — | Cada gráfico tiene título, ejes y una interpretación de 2–3 frases | ⬜ |
| **F9** Informe | Las 8 secciones del punto 6, con fragmentos de código comentados y referencias APA o IEEE | D8 | Lista de las 8 secciones marcada; cada afirmación numérica sale del CSV de F7 | ⬜ |
| **F10** README y prueba limpia | README de instalación en Windows; el dueño del segundo equipo Windows lo sigue paso a paso sin ayuda | D2 | Ambas versiones corren en un Windows limpio solo con el README | ⬜ |
| **F11** Sustentación | Guion de la demo con una muestra pequeña; ronda de preguntas entre todos | — | Demo completa en el tiempo previsto; cada integrante explica una parte que no escribió | ⬜ |

### Reparto entre 3 personas

Roles, no nombres: se asignan en F0. P1 debe ser el dueño del equipo Windows de prueba y P2 el del otro Windows.

| Persona | Rol | Fases propias | Secciones del informe | Revisa a |
|---|---|---|---|---|
| **P1** (Windows de prueba) | Entorno, datos y medición | F0 (instalación en Windows), F1, F7, README de F10 | 3 Entorno, 5 Evidencia de inviabilidad | P3 |
| **P2** (segundo Windows) | Versión pandas | F3, F4, F8, prueba limpia de F10 | 1 Introducción, 4 Código (pandas), 6 Comparativo | P1 |
| **P3 — Cristhian** (Linux) | Versión PySpark | F0 (repositorio), F5, F6, script de medición (D6) | 2 Marco teórico, 4 Código (Spark), 7 Conclusiones | P2 |
| **Los tres** | — | F2, F9 (ensamblar y referencias), F11 | 8 Referencias | — |

Orden y trabajo en paralelo:
1. **F0 y F2 primero, entre los tres.** F2 es el contrato: archivos de entrada, columnas, y el esquema exacto de cada tabla de salida.
2. **Después, en paralelo:** P1 hace F1, P2 hace F3–F4, P3 hace F5–F6 y el script de medición. Nadie espera a nadie porque todos programan contra el contrato de F2 y la muestra del 1 %.
3. **F7 necesita F4, F6 y el script.** Es el único punto donde P1 depende de los otros dos.
4. **F8 a F11 al final**, con los datos de F7.

### Relevo: cómo sigue el siguiente compañero

1. **Este `PLAN.md` vive en el repositorio y es la única fuente de estado.** Quien termina una fase cambia ⬜ por ✅ en el mismo cambio que entrega el trabajo.
2. **Una fase = una rama = un pull request** hacia `main`. Nadie escribe directo en `main`.
3. **Cada pull request lleva una nota de relevo** con cuatro puntos: qué se hizo, cómo se ejecuta (comando exacto), qué se verificó (el criterio de la fase, con la salida) y qué queda pendiente o qué problema se encontró.
4. **Una fase está terminada cuando el revisor la ejecuta en su propio equipo** y se cumple el criterio. No basta con que funcione en el equipo del autor.
5. **La revisión es rotativa** (columna "Revisa a"), para que cada uno conozca código que no escribió. Lo exige la sustentación.
6. **Las dificultades se anotan en `docs/BITACORA.md`** el mismo día, con fecha y solución. Alimenta la sección 3 del informe.
7. **Si alguien se bloquea más de un día,** lo escribe en el pull request y otro toma la fase leyendo la nota de relevo.

### Reparto final (2 personas, 2026-11-17)

El trabajo efectivo queda entre dos personas. Tareas detalladas de Daniel en [`docs/TAREAS_DANIEL.md`](docs/TAREAS_DANIEL.md).

| Persona | Hace |
|---|---|
| **Daniel** (Windows, datos) | Volumen con el JSONL de 31 GB para que pandas falle (T1), capturas (T2), mitigaciones de pandas (F4, T3), medición de la versión optimizada (T4) |
| **Cristhian** (Linux) | Spark optimizado (F6), gráficos (F8), informe PDF (F9) |

Por el plazo, en las muestras del 50 % y 100 % se acepta **una** repetición, y se explica en el informe.
