# Contrato del análisis (fase F2)

**Estado: PROPUESTA.** Pasa a "aprobado" cuando las tres personas del grupo lo acepten en el pull request.

Este documento es lo único que la versión pandas y la versión PySpark deben compartir. Si las dos lo cumplen, producen resultados equivalentes aunque se programen por separado.

## 1. Caso de uso

**Dominio:** movilidad urbana. Viajes de taxi amarillo de Nueva York, publicados por la NYC Taxi & Limousine Commission (TLC).

**Problema:** una autoridad de transporte quiere saber dónde y cuándo se concentra la demanda y el ingreso, para decidir dónde reforzar el servicio.

| # | Pregunta | Tabla de salida |
|---|---|---|
| P1 | ¿Qué zonas generan más viajes e ingresos? | `revenue_by_zone` |
| P2 | ¿Cómo cambia la demanda según el día de la semana y la hora? | `demand_by_hour` |
| P3 | ¿Cómo evolucionan mes a mes los viajes, el ingreso y la propina en cada distrito? | `monthly_by_borough` |

## 2. Datos de entrada

| Archivo | Origen | Formato |
|---|---|---|
| `data/raw/yellow_tripdata_AAAA-MM.parquet` | `https://d37ci6vzurychx.cloudfront.net/trip-data/` | Parquet, un archivo por mes |
| `data/raw/taxi_zone_lookup.csv` | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` | CSV, 265 filas, 12 KB |

**Periodo propuesto: 2018 y 2019** (24 archivos). Son años completos, anteriores a la pandemia y con el mismo esquema.

### Tamaño (medido el 2026-11-12)

| Medida | Valor | Cómo se obtuvo |
|---|---|---|
| Enero de 2019 en disco | 105,3 MB | Archivo descargado |
| Enero de 2019, filas | 7 696 617 | Metadatos del Parquet |
| Enero de 2019 en memoria con pandas | 1 241 MB | `memory_usage(deep=True)` |
| Relación memoria / disco | 11,8 veces | Cálculo sobre ese mes |
| 2018 en disco | 1 396 MB | Cabeceras HTTP de los 12 archivos |
| 2019 en disco | 1 185 MB | Cabeceras HTTP de los 12 archivos |
| **2018 + 2019 en disco** | **2 581 MB** | Suma |
| **2018 + 2019 en memoria con pandas** | **≈ 30 GB (estimado)** | 2 581 MB × 11,8 |

La estimación de 30 GB sale de un solo mes. P1 la confirma en la fase F1 con los 24 archivos. Si el equipo de prueba tiene 32 GB de RAM o más, se agrega 2017 (1 520 MB en disco).

### Columnas que se usan

Solo estas. Las demás columnas del archivo se ignoran.

| Columna | Tipo | Uso |
|---|---|---|
| `tpep_pickup_datetime` | fecha y hora | Limpieza, día, hora, mes |
| `tpep_dropoff_datetime` | fecha y hora | Duración |
| `trip_distance` | decimal (millas) | Limpieza, promedio |
| `PULocationID` | entero | Llave de la unión con las zonas |
| `payment_type` | entero (1 = tarjeta) | Propina |
| `fare_amount` | decimal | Limpieza, propina, promedio |
| `tip_amount` | decimal | Propina |
| `total_amount` | decimal | Limpieza, ingreso |

De la tabla de zonas: `LocationID`, `Borough`, `Zone`.

## 3. Muestras

Las muestras son **archivos ya generados**, no un muestreo que haga cada versión. Así pandas y Spark leen exactamente las mismas filas.

| Carpeta | Contenido |
|---|---|
| `data/samples/pct_001/` | 1 % de las filas |
| `data/samples/pct_010/` | 10 % |
| `data/samples/pct_050/` | 50 % |
| `data/samples/pct_100/` | 100 % |

Regla: de cada archivo mensual se conservan las filas cuya posición cumple `posición % 100 < porcentaje`. Es determinista (siempre da lo mismo) y anidada (el 1 % está dentro del 10 %). Cada carpeta conserva un Parquet por mes, con el esquema original. El script lo escribe P1 en la fase F1.

## 4. Limpieza

Una fila se conserva solo si cumple **todas** las reglas. La columna "Filas que fallan" es lo observado en enero de 2019.

| # | Regla | Motivo | Filas que fallan |
|---|---|---|---|
| L1 | `2018-01-01 <= tpep_pickup_datetime < 2020-01-01` | Hay fechas imposibles (de 2001 a 2088) | 77 |
| L2 | `0 < duration_min <= 180` | Duraciones negativas, cero o de varios días | 27 471 |
| L3 | `0 < trip_distance <= 100` | Viajes sin distancia o con distancias irreales | 55 121 |
| L4 | `fare_amount > 0` | Tarifas en cero o negativas (devoluciones) | 9 770 |
| L5 | `total_amount > 0` | Totales en cero o negativos | 8 770 |

Una misma fila puede fallar varias reglas, así que los conteos no se suman. En total, en enero de 2019 se descartan 83 098 de 7 696 617 filas (1,08 %) y quedan 7 613 519.

Con esas filas limpias, `revenue_by_zone` tiene 261 filas y `demand_by_hour` tiene 168. Sirven como valores de control al programar.

## 5. Columnas derivadas

| Columna | Definición |
|---|---|
| `duration_min` | `(tpep_dropoff_datetime - tpep_pickup_datetime)` en segundos, dividido entre 60 |
| `pickup_hour` | Hora del `tpep_pickup_datetime`, de 0 a 23 |
| `day_of_week` | Día ISO: **1 = lunes … 7 = domingo** |
| `year_month` | Texto `AAAA-MM` del `tpep_pickup_datetime` |

`duration_min` se calcula antes de la limpieza, porque la regla L2 la necesita.

Cuidado con `day_of_week`: pandas numera de 0 (lunes) a 6, y la función `dayofweek` de Spark de 1 (domingo) a 7. Las dos versiones deben convertir a la numeración ISO de esta tabla.

Las horas no se convierten de zona horaria: se usan tal como vienen en el archivo.

## 6. Unión

Unión **izquierda** de los viajes con la tabla de zonas: `PULocationID = LocationID`.

Si `Borough` o `Zone` quedan vacíos, se reemplazan por el texto `Unknown`. La tabla de zonas trae dos filas incompletas (264 y 265).

## 7. Tablas de salida

Reglas comunes:
- Los promedios se calculan como suma dividida entre cantidad, sobre las filas limpias.
- Los decimales se redondean a **2 cifras**, al final.
- Los nombres de columna y su orden son exactamente los de las tablas.
- Formato de salida: Parquet, en `output/<motor>/<muestra>/<tabla>/`, con `<motor>` igual a `pandas` o `spark`.

### `revenue_by_zone` (P1)

Agrupada por `borough`, `zone`. Orden: `total_revenue` descendente, luego `zone`.

| Columna | Tipo | Definición |
|---|---|---|
| `borough` | texto | Distrito de recogida |
| `zone` | texto | Zona de recogida |
| `trips` | entero | Cantidad de viajes |
| `total_revenue` | decimal | Suma de `total_amount` |
| `avg_fare` | decimal | Promedio de `fare_amount` |
| `avg_distance` | decimal | Promedio de `trip_distance` |

### `demand_by_hour` (P2)

Agrupada por `day_of_week`, `pickup_hour`. Orden: `day_of_week`, `pickup_hour`. Máximo 168 filas.

| Columna | Tipo | Definición |
|---|---|---|
| `day_of_week` | entero | 1 a 7 |
| `pickup_hour` | entero | 0 a 23 |
| `trips` | entero | Cantidad de viajes |
| `avg_duration_min` | decimal | Promedio de `duration_min` |
| `avg_total` | decimal | Promedio de `total_amount` |

### `monthly_by_borough` (P3)

Agrupada por `year_month`, `borough`. Orden: `year_month`, `borough`.

| Columna | Tipo | Definición |
|---|---|---|
| `year_month` | texto | `AAAA-MM` |
| `borough` | texto | Distrito de recogida |
| `trips` | entero | Cantidad de viajes |
| `total_revenue` | decimal | Suma de `total_amount` |
| `avg_tip_pct_card` | decimal | Solo viajes con `payment_type = 1`: `100 × suma(tip_amount) / suma(fare_amount)`. Vacío si el grupo no tiene viajes con tarjeta |

## 8. Cómo se ejecuta cada versión

Las dos versiones se llaman igual. El script de medición depende de esto.

```
python -m pandas_version.run --input data/samples/pct_010 --zones data/raw/taxi_zone_lookup.csv --output output/pandas/pct_010
python -m spark_version.run  --input data/samples/pct_010 --zones data/raw/taxi_zone_lookup.csv --output output/spark/pct_010
```

- Código de salida 0 si terminó bien; distinto de 0 si falló.
- No se imprime nada más que mensajes de avance.

## 9. Cuándo dos resultados son equivalentes

Para cada tabla, entre `output/pandas/...` y `output/spark/...`:

1. Mismas columnas, en el mismo orden.
2. Misma cantidad de filas.
3. Mismas llaves de agrupación.
4. Columnas enteras idénticas.
5. Columnas decimales con diferencia máxima de 0,01.

## 10. Operaciones mínimas del enunciado

| Operación exigida | Dónde se cumple |
|---|---|
| Lectura de datos | 24 archivos Parquet y un CSV (sección 2) |
| Limpieza | Reglas L1 a L5 (sección 4) |
| Transformaciones | 4 columnas derivadas (sección 5) |
| Unión (join) | Viajes con zonas (sección 6); la usan P1 y P3 |
| Agregaciones por grupos | Las tres tablas (sección 7) |
| Escritura de resultados | Parquet (sección 7) |
