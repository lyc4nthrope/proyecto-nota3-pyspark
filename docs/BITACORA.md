# Bitácora

Dificultades encontradas y cómo se resolvieron. Se anota el mismo día. Alimenta la sección 3 del informe (entorno y configuración).

| Fecha | Quién | Equipo | Problema | Solución |
|---|---|---|---|---|
| 2026-11-12 | Cristhian | Linux (Debian), Python 3.13.5, JDK 17.0.18 | PySpark 4.3.0 exige Python 3.11 o superior; el enunciado dice "3.9 o superior" | Se fija Python 3.11–3.13 y JDK 17 para todo el grupo (decisión D3 del plan) |
| 2026-11-12 | Cristhian | Linux (Debian) | Un mes de viajes pesa 105 MB en disco pero 1 241 MB en memoria con pandas (11,8 veces) | Se dimensiona el conjunto por memoria y no por disco: 2018 + 2019 son 2,6 GB en disco y unos 30 GB estimados en pandas |
| 2026-11-12 | Cristhian | Linux (Debian) | pandas y Spark numeran distinto el día de la semana | El contrato fija la numeración ISO (1 = lunes) para las dos versiones |
