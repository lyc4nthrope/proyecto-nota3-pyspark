# Datos de los gráficos

Generado por `docs/graficos/make_charts.py` desde `docs/evidencia/measurements_raw_text.csv`.
Tiempo total de una corrida = suma de sus operaciones; pico = el mayor pico entre ellas.
Las corridas cortadas por la guardia de RAM no entran en ningún promedio.
GB con 1 GB = 1024 MB.

## Corridas terminadas

| Muestra | Motor | Corridas | Tiempo medio (s) | Mín (s) | Máx (s) | Pico medio (GB) | Pico máx (GB) |
|---|---|---|---|---|---|---|---|
| 1 % | pandas | 3 | 4.7 | 4.0 | 5.7 | 1.29 | 1.30 |
| 1 % | PySpark | 3 | 52.7 | 50.9 | 54.8 | 3.43 | 3.75 |
| 10 % | pandas | 3 | 27.8 | 26.4 | 29.8 | 4.54 | 4.58 |
| 10 % | PySpark | 3 | 106.8 | 87.6 | 132.5 | 4.03 | 4.06 |
| 50 % | PySpark | 1 | 433.3 | 433.3 | 433.3 | 5.16 | 5.16 |
| 100 % | PySpark | 1 | 705.7 | 705.7 | 705.7 | 5.76 | 5.76 |

## Corridas cortadas (no terminaron)

| Muestra | Motor | Rep. | Memoria privada al corte (GB) | Registro |
|---|---|---|---|---|
| 50 % | pandas | 1 | 12.68 | killed at system RAM 97% (available 506 MB); process tree 12984.2 MB private (10384.0 MB resident) at kill |
| 50 % | pandas | 2 | 12.66 | killed at system RAM 98% (available 390 MB); process tree 12965.5 MB private (10548.5 MB resident) at kill |
| 50 % | pandas | 3 | 12.82 | killed at system RAM 98% (available 260 MB); process tree 13130.7 MB private (10371.5 MB resident) at kill |
| 100 % | pandas | 1 | 13.57 | killed at system RAM 99% (available 138 MB); process tree 13894.2 MB private (8344.4 MB resident) at kill |
| 100 % | pandas | 2 | 14.24 | killed at system RAM 99% (available 114 MB); process tree 14579.8 MB private (8434.4 MB resident) at kill |
| 100 % | pandas | 3 | 13.83 | killed at system RAM 98% (available 239 MB); process tree 14162.1 MB private (10846.3 MB resident) at kill |

## Tiempo Spark ÷ tiempo pandas (donde ambos terminan)

| Muestra | Veces |
|---|---|
| 1 % | 11.3 |
| 10 % | 3.8 |

## Tiempo medio por operación al 10 % (s)

| Operación | pandas | PySpark |
|---|---|---|
| Lectura | 2.5 | 7.7 |
| Limpieza | 0.1 | 4.0 |
| Columnas derivadas | 8.0 | 2.8 |
| Join con catálogo | 11.5 | 11.5 |
| Agregado por tienda | 4.6 | 29.0 |
| Agregado temporal | 0.8 | 19.5 |
| Escritura Parquet | 0.2 | 32.3 |
