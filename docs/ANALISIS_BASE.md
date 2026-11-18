# Cómo empezar — bases del análisis (decisión D1 abierta)

**Estado:** borrador de arranque. El caso de uso y el conjunto de datos (**D1**) siguen **sin decidir**: nada de este documento nombra ni compromete una fuente concreta. Se cierra en F2, cuando el grupo elija D1 y se rellenen los huecos marcados como `<sin decidir>`.

Sirve para arrancar en paralelo sin esperar a D1: mientras no se elija el dataset, cada persona puede preparar su parte contra este contrato genérico.

## 1. Qué hay que poder responder (preguntas de negocio)

Toda versión del proyecto debe contestar preguntas como estas, sobre el dominio que se elija:

1. ¿Cómo se comporta la demanda a lo largo del tiempo (por mes, día de la semana, hora)?
2. ¿Qué grupos (categorías, zonas, tipos) concentran el mayor volumen y el mayor valor total?
3. ¿Cuál es el promedio y la dispersión de la variable principal dentro de cada grupo?
4. ¿Qué registros están incompletos o son incoherentes y cuánto pesan sobre el resultado?
5. ¿Qué relación existe entre una entidad principal y su catálogo/ dimensión (la tabla con la que se hará el *join*)?

Si al elegir D1 alguna de estas cinco no se puede responder con los datos disponibles, el candidato no sirve.

## 2. Operaciones mínimas que exige el enunciado

Cada operación necesita al menos una pregunta de la sección 1 que la use. Este es el mínimo que **todo** candidato a dataset debe soportar:

| # | Operación | Pregunta que la usa | Qué deja sin decidir D1 |
|---|---|---|---|
| 1 | Lectura del origen (CSV/Parquet) | — | formato y rutas |
| 2 | Limpieza (nulos, tipos, rangos inválidos) | 4 | qué columna es la principal, qué es inválido |
| 3 | Columnas derivadas (a partir de fecha/hora) | 1 | qué fechas hay |
| 4 | **Al menos un *join*** con una tabla pequeña | 5 | las dos tablas y la llave |
| 5 | Agregaciones por grupo (`groupBy`) | 2, 3 | qué columna agrupa |
| 6 | Escritura en **Parquet** | — | nombre y esquema de la salida |
| 7 | **Al menos una optimización** (particionado, caché, Parquet, *broadcast join*) | — | cuál, según el patrón de los datos |

## 3. Criterios antes de elegir el dataset (checklist de D1)

- **Volumen:** el tamaño en RAM estimado debe **superar la RAM del equipo de prueba** (15,3 GB en el equipo donde se preparó este entorno; el equipo de prueba se fija en D2) o hacer fallar a pandas. Sin esto hay un 0–2,9 en código.
- **Disco:** la descarga debe caber en el equipo con **menos espacio libre**. Aquí hay 300 GB libres, pero en el equipo Linux quedan ~11 GB: ese es el límite real de la descarga.
- **Fuente pública y descargable por script:** los datos nunca se suben al repositorio (ver `data/README.md`).
- **Dos tablas como mínimo:** una principal y una de catálogo pequeña, para que el *join* sea real (y pueda ser *broadcast join*).
- **Columnas mínimas:** una fecha/hora, al menos una categoría y al menos un valor numérico → cubre limpieza, derivadas y agregaciones.
- **Documentación y licencia:** descripción de las columnas y permiso de uso.
- **Mismo origen para ambas versiones** (D5): un único formato de entrada, CSV o Parquet, elegido en F1 según el espacio.

## 4. Contrato F2 (plantilla: se rellena al decidir D1)

> Este es el contrato contra el que se programa en paralelo. Hasta cerrar D1 solo existe su forma; los nombres con `<sin decidir>` se sustituyen en F2.

### 4.1 Entrada

- Archivos: `<sin decidir>` (mismo conjunto para pandas y para PySpark)
- Columnas: `<sin decidir>`

### 4.2 Limpieza

- Excluir filas con `<sin decidir>` nulo o fuera de rango.
- Convertir `<sin decidir>` a tipo numérico; fechas a `date`/`timestamp`.

### 4.3 Columnas derivadas

- `<sin decidir>` (por ejemplo: mes, día de la semana, hora).

### 4.4 Join

- Izquierda: `<sin derecha>` (tabla principal) con derecha: `<sin decidir>` (catálogo pequeño).
- Llave: `<sin decidir>`; tipo de unión: izquierda.
- La tabla derecha es candidata a **broadcast join** por ser pequeña.

### 4.5 Salidas (esquema exacto, una fila por tabla)

| Tabla | Columnas | Tipo | Descripción |
|---|---|---|---|
| `<sin decidir>` | `<sin decidir>` | `<sin decidir>` | agregado por grupo |
| `<sin decidir>` | `<sin decidir>` | `<sin decidir>` | resumen temporal |

### 4.6 Preguntas → operaciones

| Pregunta (sección 1) | Operación (sección 2) |
|---|---|
| 1 | 3, 5 |
| 2, 3 | 5 |
| 4 | 2 |
| 5 | 4 |

## 5. Orden de arranque

1. **F0 (entorno) y F2 (contrato) primero, entre los tres.** F2 convierte esta plantilla en el contrato concreto.
2. **En paralelo:** F1 (datos), F3–F4 (pandas), F5–F6 (PySpark) y el script de medición. Todos programan contra el contrato de F2 y la muestra del 1 %.
3. **F7** (medición) espera a F4, F6 y al script: es el único punto de dependencia.
4. **F8 a F11** al final, con los datos de F7.

## 6. Qué queda por decidir

| # | Decisión | Estado |
|---|---|---|
| D1 | Caso de uso y conjunto de datos | **Abierta** — se cierra en F2 con el checklist de la sección 3 |
| D2 | Cuál de los dos equipos Windows es el de prueba | Abierta — mientras tanto, todo lo de este documento vale para cualquier equipo |
