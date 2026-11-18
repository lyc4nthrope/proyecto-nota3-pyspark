# Datos de los gráficos

Generado por `docs/graficos/make_charts.py` desde `docs/evidencia/measurements_ladder.csv`.
Tiempo total de una corrida = suma de sus operaciones; pico = el mayor pico entre ellas.
GB con 1 GB = 1024 MB (el reporte de Daniel divide entre 1000: 10 172 MB = 9,93 GB aquí y 10,2 GB allá).

| Muestra | Motor | Corridas | Tiempo medio (s) | Mín (s) | Máx (s) | Pico medio (GB) | Pico máx (GB) |
|---|---|---|---|---|---|---|---|
| 1 % | pandas | 3 | 4.9 | 4.6 | 5.4 | 1.11 | 1.13 |
| 1 % | PySpark | 3 | 72.9 | 70.4 | 77.6 | 3.04 | 3.11 |
| 10 % | pandas | 3 | 25.7 | 24.9 | 26.8 | 2.75 | 2.77 |
| 10 % | PySpark | 3 | 121.3 | 119.4 | 122.9 | 4.64 | 4.90 |
| 50 % | pandas | 3 | 143.1 | 130.8 | 159.0 | 7.26 | 7.92 |
| 50 % | PySpark | 3 | 241.1 | 200.5 | 263.5 | 6.22 | 6.40 |
| 100 % | pandas | 3 | 205.1 | 188.1 | 236.5 | 9.93 | 10.00 |
| 100 % | PySpark | 3 | 311.1 | 257.3 | 342.5 | 7.49 | 7.97 |

Tiempo medio por operación al 100 % (s):

| Operación | pandas | PySpark |
|---|---|---|
| Lectura | 4.2 | 5.5 |
| Limpieza | 1.0 | 6.6 |
| Columnas derivadas | 39.6 | 5.3 |
| Join con catálogo | 104.8 | 35.4 |
| Agregado por tienda | 46.3 | 92.6 |
| Agregado temporal | 8.9 | 59.9 |
| Escritura Parquet | 0.2 | 105.9 |
