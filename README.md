# Proyecto Nota 3 — Big Data con Python y PySpark

Comparación medible entre una solución con **pandas** y otra con **PySpark** sobre un conjunto de datos que pandas no puede procesar de forma viable.

Estado y reparto del trabajo: [`PLAN.md`](PLAN.md).

## Estructura

| Carpeta | Contenido |
|---|---|
| `pandas_version/` | Versión tradicional con pandas |
| `spark_version/` | Versión con PySpark |
| `benchmark/` | Mediciones y comparación pandas ↔ PySpark (guía en `benchmark/README.md`) |
| `tests/` | Pruebas automáticas |
| `scripts/` | Utilidades de preparación del entorno |
| `data/` | Datos (no se suben; ver `data/README.md`) |
| `docs/` | Bitácora e informe |

## Requisitos

| Software | Versión |
|---|---|
| Python | 3.11 a 3.13 |
| Java (JDK) | 17 |
| PySpark | 4.3.0 (se instala con `requirements.txt`) |

## Instalación en Windows 10/11

Funciona en cualquier equipo Windows 10/11; solo cambian las rutas exactas del JDK. Abrir **PowerShell como usuario normal** (el paso 2 pedirá elevación).

### 1. Python 3.13

```powershell
winget install --id Python.Python.3.13 -e
py -3.13 --version      # debe imprimir 3.13.x
```

En Windows 11, `python` suele abrir la tienda Microsoft Store: usar siempre el lanzador **`py -3.13`**.

### 2. JDK 17

```powershell
winget install --id Microsoft.OpenJDK.17 -e     # muestra el diálogo de elevación (UAC)
```

Abrir una **terminal nueva** después de instalar, para que `JAVA_HOME` exista, y comprobar:

```powershell
java -version        # debe imprimir 17.x
echo $env:JAVA_HOME  # C:\Program Files\Microsoft\jdk-17.x.y.z
```

`SPARK_HOME` **no** hace falta: `requirements.txt` instala PySpark con Spark incluido.

### 3. winutils (solo Windows)

Sin estos binarios, escribir Parquet falla con `UnsatisfiedLinkError` o con
`Could not locate executable ... winutils.exe in HADOOP_HOME`.

```powershell
.\scripts\setup_winutils.ps1
```

El script crea `%LOCALAPPDATA%\hadoop\bin` (**no** pide administrador), descarga
`winutils.exe` y `hadoop.dll` de Hadoop 3.3.6, comprueba su SHA-256 y define
`HADOOP_HOME` + `PATH` a nivel de usuario. **Abrir una terminal nueva** después
de ejecutarlo.

### 4. Entorno de Python y dependencias

```powershell
py -3.13 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

## Instalación en Linux (solo desarrollo)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Comprobar la instalación

```powershell
.venv\Scripts\python -m pytest -q
```

```bash
pytest -q
```

Si **todas** las pruebas pasan, Java, Spark y la escritura en Parquet funcionan en ese equipo. Este es el paso que ejecuta el revisor en **cualquiera** de los tres equipos antes de aceptar una fase.

## Cómo trabajamos

- Una fase del plan = una rama = un pull request hacia `main`.
- Cada pull request lleva la nota de relevo de la plantilla.
- Una fase termina cuando el revisor la ejecuta en su propio equipo.

## Cómo ejecutar

La **primera ejecución descarga datos** (unos 16 GB en total) y construye las
muestras; las siguientes reutilizan todo lo ya procesado. El detalle de
rutas, caché y opciones está en [`benchmark/README.md`](benchmark/README.md).

### Primera vez (en frío)

```powershell
# Sondeo de perfil: descarga la tabla de interacciones (3,9 GB) y parte del
# metadata (~0,3 GB), y mide sobre una muestra del 1 %
.venv\Scripts\python -m benchmark.measure_sample

# Comparación pandas vs PySpark: añade el metadata completo (~11,8 GB),
# proyecta el catálogo y construye las muestras 1/10/50/100 %
.venv\Scripts\python -m benchmark.compare
```

Ambos comparten la tabla de interacciones: ejecutar los dos no duplica esa
descarga.

### Repetición

Los **mismos comandos** reutilizan automáticamente lo ya descargado y las
muestras ya construidas (se valida por tamaño de archivo y por manifiesto, no
por fecha). Para ir directo a las corridas sin reconstruir muestras:

```powershell
.venv\Scripts\python -m benchmark.compare --skip-build
```

Cada repetición de medición corre en su propio proceso; no hace falta limpiar
nada entre corridas.

### Con datos ya procesados

Si `data/raw/amazon_hk/` y `data/samples/ladder/` ya están poblados (por
ejemplo, copiados de otro equipo que hizo la primera ejecución), **todo
funciona sin red**. La versión de referencia de pandas:

```powershell
.venv\Scripts\python -m pandas_version.pipeline
```

Para **forzar** una reconstrucción desde cero: `--force-download` y
`--rebuild-sample` en la sonda, o borrar el manifiesto `.manifest.json`
del artefacto que se quiera regenerar (ver `benchmark/README.md`).

Los datos nunca se suben al repositorio (`data/` está en `.gitignore`); los
resultados se regeneran siempre en `output/`.

## Datos

*Amazon Reviews 2023* (McAuley Lab, UC San Diego), categoría **Home and Kitchen**, publicado en
Hugging Face (`McAuley-Lab/Amazon-Reviews-2023`). Los scripts de `benchmark/` los descargan por URL
directa; no se ejecuta código del repositorio de origen.

| Archivo | Contenido | Tamaño |
|---|---|---|
| `0core/rating_only` (CSV) | Interacciones usuario–producto (66,6 M filas) | 3,86 GB |
| `meta_Home_and_Kitchen` (JSONL) | Catálogo de productos (3.735.584) | 11,8 GB |
| Reseñas completas `Home_and_Kitchen` (JSONL) | 67.409.944 reseñas **con texto**: la ruta del informe | 31,4 GB |

## Reproducir los resultados del informe (ruta con texto)

Las cifras de las secciones 5 y 6 del informe salen de estos comandos (las fracciones van de 0 a 1: `1.0` = 100 %):

```powershell
# Escalera 1/10/50/100 % con el texto: descarga reanudable, conversión a Parquet y mediciones
.venv\Scripts\python -m benchmark.compare --raw
# Intento ingenuo: pd.read_json sobre los 31 GB (termina cortado por la guardia de RAM)
.venv\Scripts\python -m benchmark.naive_attempt
# Mitigaciones de pandas (columnas, tipos, chunks) por nivel
.venv\Scripts\python -m benchmark.run_mitigations
# Versión Spark optimizada, de principio a fin, al 100 %
.venv\Scripts\python -m benchmark.measure_optimized --level 1.0 --rep 1
# Gráficos del informe
.venv\Scripts\python docs\graficos\make_charts.py
```

La versión optimizada también se puede correr sola:

```powershell
.venv\Scripts\python -m spark_version.run --reviews <muestra.parquet> --catalog data\samples\ladder\catalog.parquet --output output\spark_optimized
```

Evidencia ya generada: `docs/evidencia/` (CSV de mediciones, mitigaciones y capturas). Informe: `docs/informe/es/informe.pdf` y `docs/informe/en/report.pdf`.
