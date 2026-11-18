# Mitigaciones de pandas sobre la ruta con texto

Generado: 2026-11-18T06:27:25+00:00

Una repetición por técnica y nivel, con el mismo instrumento que la escalera (proceso propio, guardia de RAM, enfriamiento entre corridas).

| Técnica | Nivel | Filas | Estado | Tiempo (s) | Memoria | Notas |
|---|---|---|---|---|---|---|
| columns | 1pct | 673,379 | termino | 5.299 | 251.7 | text is a contract column, so it cannot be dropped |
| dtypes | 1pct | 673,379 | termino | 7.295 | 227.7 | text cannot be narrowed: its bytes are the data |
| chunks | 1pct | 750,000 | termino | 67.062 | 36.8 | exact products needs 321,585 ASINs alive as sets per store; global operations (sort, percentiles, windows) cannot be chunked |
| columns | 10pct | 6,742,408 | termino | 25.778 | 2519.5 | text is a contract column, so it cannot be dropped |
| dtypes | 10pct | 6,742,408 | termino | 20.305 | 2220.6 | text cannot be narrowed: its bytes are the data |
| chunks | 10pct | 6,750,000 | termino | 835.693 | 141.0 | exact products needs 1,231,703 ASINs alive as sets per store; global operations (sort, percentiles, windows) cannot be chunked |
| columns | 50pct | 33,706,117 | killed at system RAM 99% (available 152 MB) | - | 11465.0 | killed at system RAM 99% (available 152 MB) |
| dtypes | 50pct | 33,706,117 | killed at system RAM 98% (available 259 MB) | - | 11568.6 | killed at system RAM 98% (available 259 MB) |
| chunks | 50pct | 33,750,000 | termino | 2401.499 | 314.8 | exact products needs 2,750,365 ASINs alive as sets per store; global operations (sort, percentiles, windows) cannot be chunked |
| columns | 100pct | 67,409,944 | killed at system RAM 98% (available 293 MB) | - | 10663.0 | killed at system RAM 98% (available 293 MB) |
| dtypes | 100pct | 67,409,944 | killed at system RAM 97% (available 503 MB) | - | 10222.9 | killed at system RAM 97% (available 503 MB) |
| chunks | 100pct | 67,409,944 | termino | 3459.839 | 427.4 | exact products needs 3,734,414 ASINs alive as sets per store; global operations (sort, percentiles, windows) cannot be chunked |

## Por qué ninguna resulta práctica

- **Selección de columnas** y **tipos optimizados** terminan hasta el 10 %, pero mueren al 50 % y al 100 %: el texto de la reseña es columna del contrato (para `avg_text_length`), no se puede descartar ni estrechar, y son sus bytes los que llenan la memoria (10–11 GB privados al cortar).
- **Lectura por *chunks*** es la única que termina el 100 %, pero a un precio que ninguna alternativa paga: ~58 minutos contra los ~3,6 minutos de una pasada de Spark (unas 16 veces más lento), reteniendo los conjuntos de ASIN por tienda (3.734.414 productos vivos, ~427 MB) para calcular `products`, y sin poder expresar operaciones globales —ordenar 67 millones de filas, percentiles, ventanas— sin volver a leerlo todo. El código deja de ser una agregación y pasa a sumas parciales, suma de cuadrados y fusión de acumuladores.

Ninguna acerca a pandas a terminar el 100 % en un tiempo práctico: o no termina (columnas, tipos), o termina 16 veces más lento y con parte de la lógica reescrita a mano (*chunks*). Ese es el argumento de inviabilidad **aun mitigado** que exige el enunciado.

Ver tabla por operación en el reporte de la escalera.
