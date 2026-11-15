# Proyecto Nota 3 — Big Data con Python y PySpark

Comparación medible entre una solución con **pandas** y otra con **PySpark** sobre un conjunto de datos que pandas no puede procesar de forma viable.

Estado y reparto del trabajo: [`PLAN.md`](PLAN.md).

## Estructura

| Carpeta | Contenido |
|---|---|
| `pandas_version/` | Versión tradicional con pandas |
| `spark_version/` | Versión con PySpark |
| `benchmark/` | Script de medición de tiempo y memoria |
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
