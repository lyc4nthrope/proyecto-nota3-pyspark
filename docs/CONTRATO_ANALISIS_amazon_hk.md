# Contrato del análisis — borrador v0 (línea Amazon Home_and_Kitchen)

**Estado:** borrador. Se cierra cuando se resuelvan los puntos abiertos de la
última sección. Es el documento contra el que programan las dos versiones
(pandas y PySpark): todo lo que dice es obligatorio para ambas, y lo que no
dice no está decidido.

**Relación con la otra propuesta:** el grupo tiene un contrato propuesto para
**NYC TLC Yellow Taxi** en la rama `docs/f2-analysis-contract`. Este documento
es el contrato de la línea **Amazon Home_and_Kitchen**, que es la que está
construida y medida. Sirve para comparar las dos líneas con el mismo detalle;
si el grupo cierra por taxi, este documento se archiva y el contrato oficial es
el de la otra rama.

**Alcance:** análisis de reseñas de productos de Amazon, dominio
`Home_and_Kitchen`. Si el grupo cierra la elección de conjunto por otra
fuente, este documento se reemplaza por el contrato equivalente.

## 1. Fuentes

| Fuente | Archivo | Tamaño | Contenido |
|---|---|---|---|
| Hechos | `benchmark/0core/rating_only/Home_and_Kitchen.csv` | 3,86 GB | 66.623.880 interacciones usuario-producto |
| Catálogo | `raw/meta_categories/meta_Home_and_Kitchen.jsonl` | 11,8 GB | 3.735.584 productos (se proyecta a Parquet) |

- Origen: `McAuley-Lab/Amazon-Reviews-2023` en Hugging Face (descarga por URL
  directa, sin ejecutar código remoto del dataset).
- **Mismo origen para ambas versiones:** sí. pandas y PySpark leen los mismos
  Parquet de muestra y de catálogo; ninguna versión lee un formato que la otra
  no lee.
- Licencia y cita del dataset: pendiente de redactar para las referencias.

## 2. Esquema de entrada

**Interacciones (hechos):**

| Columna | Tipo | Descripción |
|---|---|---|
| `user_id` | string | Identificador del usuario |
| `parent_asin` | string | Identificador del producto (clave del join) |
| `rating` | double | Valoración del 1 al 5 |
| `timestamp` | long | Fecha de la reseña, milisegundos desde época |

**Catálogo (dimensión):**

| Columna | Tipo | Descripción |
|---|---|---|
| `parent_asin` | string | Identificador del producto (clave del join) |
| `main_category` | string | Categoría principal |
| `store` | string | Tienda o marca |
| `price` | double | Precio publicado |
| `average_rating` | double | Valoración media del producto |
| `rating_number` | long | Número de valoraciones del producto |

## 3. Reglas de limpieza (idénticas en ambos motores)

1. Descartar filas con nulo en `user_id`, `parent_asin`, `rating` o `timestamp`.
2. Conservar solo `1 <= rating <= 5`.
3. Conservar solo `timestamp > 0`.

**Evidencia medida (muestra del 1 %, 665.617 filas):** la tabla 0core viene
ya procesada arriba (deduplicada y filtrada), y se comprobó: **0 duplicados**
por (`user_id`, `parent_asin`, `timestamp`) y por las 4 columnas, **0 filas
descartadas** por nulos, **0** por rango de rating y **0** por timestamp. Las
reglas quedan por tanto como **validación** sobre esta fuente, y son
necesarias si la fuente cambia al JSONL crudo. Las reglas se aplican en el
orden 1→2→3 para poder atribuir cada descarte a su causa.

## 4. Columnas derivadas

Se calculan desde `timestamp` en milisegundos, en **UTC** (la sesión de Spark y
el cálculo de pandas quedan fijados a UTC para que coincidan):

| Columna | Tipo | Definición |
|---|---|---|
| `review_ts` | timestamp | Fecha-hora de la reseña |
| `review_date` | date | Solo la fecha |
| `review_year` | int | Año |
| `review_month` | int | Mes (1-12) |
| `review_dow` | int | Día de la semana con numeración Spark: **1=domingo … 7=sábado** |
| `review_hour` | int | Hora (0-23) |

## 5. Join

- Izquierda: interacciones. Derecha: catálogo proyectado y deduplicado por
  `parent_asin`.
- Llave: `parent_asin`. Tipo: **left**.
- Filas sin pareja: **se conservan** con las columnas del catálogo en nulo y se
  cuentan (`_meta_matched`). No se descartan: son dato, no error.
- La versión optimizada podrá usar *broadcast join*; el umbral se decide al
  medir, no aquí.

## 6. Salidas (esquema exacto)

Escritura en **Parquet**. Dos tablas:

**Tabla 1 — agregado por tienda:**

| Columna | Tipo | Definición |
|---|---|---|
| `store` | string | Tienda (nulo si el producto no tiene catálogo) |
| `reviews` | long | Número de reseñas del grupo |
| `products` | long | Productos distintos del grupo |
| `avg_rating` | double | Media de `rating`, 4 decimales |
| `stddev_rating` | double | Desviación típica **muestral** (ddof=1), 4 decimales |

**Tabla 2 — agregado temporal:**

| Columna | Tipo | Definición |
|---|---|---|
| `review_year` | int | Año |
| `review_month` | int | Mes |
| `review_dow` | int | Día de la semana (numeración Spark) |
| `reviews` | long | Número de reseñas del grupo |
| `avg_rating` | double | Media de `rating`, 4 decimales |
| `stddev_rating` | double | Desviación típica muestral, 4 decimales |

## 7. Preguntas de negocio → operaciones

| Pregunta | Operaciones que la responden |
|---|---|
| 1. ¿Cómo se comporta la demanda en el tiempo? | Columnas derivadas + agregado temporal |
| 2. ¿Qué grupos concentran mayor volumen y valor? | Join + agregado por tienda |
| 3. ¿Cuál es la media y dispersión por grupo? | Agregados (`avg_rating`, `stddev_rating`) |
| 4. ¿Qué registros son incompletos o incoherentes? | Limpieza (y perfil de calidad) |
| 5. ¿Qué relación hay entre la reseña y su catálogo? | Join y conteo de sin-pareja |

Operaciones mínimas cubiertas: lectura, limpieza, columnas derivadas, join,
agregación por grupo y escritura Parquet. La optimización se mide aparte sobre
la línea base.

## 8. Criterios de equivalencia pandas ↔ PySpark

1. Misma cantidad de filas tras la limpieza y tras el join.
2. Mismos grupos y mismos conteos (`reviews`, `products`) en ambas salidas.
3. Medias y desviaciones iguales hasta tolerancia **1e-4** (los motores redondean
   distinto en empates exactos).
4. Misma numeración de `review_dow`.
5. Misma forma de salida: mismas columnas, mismos tipos, mismo formato.
6. Verificación doble: pruebas automáticas de equivalencia sobre datos
   sintéticos y comparación de salidas en la muestra más grande que pandas
   termine.

## 9. Decisiones técnicas ya fijadas

- Zona horaria **UTC** en ambos motores.
- Desviación típica **muestral** (ddof=1), igual que `stddev` de Spark.
- Redondeo a **4 decimales** en medias y desviaciones.
- La línea base se mide **sin optimizaciones**: sin caché, sin *broadcast
  hint*, sin reparticionado y sin ajustar *shuffle*.
- Cada corrida de medición corre en un proceso nuevo.

## 10. Puntos abiertos (bloquean el cierre)

1. **Elección formal del conjunto.** Este contrato es para Amazon
   `Home_and_Kitchen`, que es la línea construida y medida. El plan original
   recomendaba NYC TLC Yellow Taxi. El grupo debe cerrar la decisión y, si
   cambia, redactar el contrato equivalente.
2. ~~**Duplicados** por (`user_id`, `parent_asin`, `timestamp`)~~ **Resuelto
   con evidencia:** no hay (0 en 665.617 filas medidas); la fuente viene
   deduplicada. La limpieza no descarta duplicados por diseño: la validación
   lo confirma.
3. ~~**Desglose de descartes por regla**~~ **Resuelto con evidencia:** 0 por
   nulos, 0 por rango de rating, 0 por fecha, sobre la misma muestra.
4. **"Valor total"** de la pregunta 2: en este dominio, ¿es la suma de ratings,
   el número de interacciones o el precio? El agregado por tienda hoy no
   incluye una columna de suma.
5. **¿Basta la tabla 0core o se necesita el JSONL crudo con texto?**
   **Resuelto por evidencia: no basta.** En la escalera completa (66,6 M
   filas) pandas **termina** el análisis en ~205 s con 10,2 GB de memoria pico
   (marco de 12,2 GB en un equipo de 15,3 GB, al límite). El volumen, tal
   cual, no justifica Spark. Queda pendiente la decisión de cómo asegurar el
   volumen: JSONL crudo con el texto en el análisis, más categorías sobre la
   tabla 0core, u otra.
6. **Tolerancia exacta** de equivalencia para las salidas (formalizar 1e-4).
7. **Licencia y cita** del dataset para las referencias del informe.
