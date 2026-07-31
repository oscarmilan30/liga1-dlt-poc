# Documento Funcional — Liga 1 DLT POC

## ¿Qué es este proyecto?

POC de ingeniería de datos sobre estadísticas históricas de la **Liga 1 Peruana de Fútbol (2020–2026)** usando **Delta Live Tables (Spark Declarative Pipelines)** con arquitectura medallion Bronze → Silver → Gold.

El objetivo principal es demostrar el uso de tecnologías modernas de Databricks — DLT, Asset Bundles y Unity Catalog — en un pipeline de datos real con datos reales.

---

## ¿Qué preguntas responde?

- ¿Qué equipo dominó históricamente la Liga 1 entre 2020 y 2026?
- ¿Cómo evolucionó el rendimiento ofensivo y defensivo de cada equipo por temporada?
- ¿Cuáles son los equipos con mayor efectividad de tiro?
- ¿Qué equipos cometen más faltas y reciben más tarjetas históricamente?
- ¿Cómo se compara el rendimiento local vs visitante por equipo?

---

## Fuente de datos

| Campo | Detalle |
|---|---|
| Fuente original | FotMob (scraping via Selenium) |
| Formato | JSON — un archivo por temporada |
| Archivos | 7 (2020–2026) |
| Total partidos | ~2,030 |
| Ruta ADLS | `liga1/dlt-poc/landing/estadisticas_partidos/` |

### Particularidades de los datos

| Problema | Descripción | Solución en Silver |
|---|---|---|
| Campos bilingües | Misma métrica en ES o EN según el partido | `coalesce(campo_ES, campo_EN)` via `en_fallback` en YAML |
| Fecha en dos formatos | `"26-01-2024"` y `"Sunday, February 13, 2022"` | Parser multi-formato |
| Fecha sin año (2026) | `"Saturday, February 14"` | Año inferido del campo `temporada` |
| Valores compuestos | `"397 (87%)"` → valor + porcentaje | Split → dos columnas `_value` y `_pct` |
| Posesión como string | `"64%"` | Extracción numérica |
| Registros sin stats | Partidos sin estadísticas disponibles | Filtrado con Expectation DLT |
| Schema variable | De 5 a 69 columnas según año e idioma | Auto Loader con schema evolution |

---

## Arquitectura Medallion

### Bronze — Ingesta Raw
- Lee los 7 JSON desde ADLS con **Auto Loader** (`cloudFiles`)
- Sin transformaciones — datos exactamente como llegaron
- Agrega metadatos: `_source_file`, `_ingestion_timestamp`, `temporada`, `fuente`

### Silver — Limpieza y Normalización
- Resuelve bilingüismo con `coalesce(ES, EN)` parametrizado desde YAML
- Renombra columnas a snake_case
- Parsea `marcador` → `goles_local` (INT) + `goles_visitante` (INT)
- Deriva `resultado`: "Local" / "Empate" / "Visitante"
- Parsea fechas en múltiples formatos → tipo `DATE`
- Separa valores compuestos `"397 (87%)"` → `INT` + `DOUBLE`
- Aplica **6 Expectations** de calidad de datos

### Gold — Valor de Negocio

| Tabla | Descripción |
|---|---|
| `gold_rendimiento_equipos` | PJ, PG, PE, PP, GF, GC, Pts por equipo y temporada |
| `gold_top_atacantes` | Equipos con más goles y mayor efectividad de tiro por temporada |
| `gold_dominio_historico` | Ranking histórico 2020–2026 acumulado |

---

## Parametrización YAML

Toda la lógica de transformación Silver está externalizada en `conf/estadisticas_partidos.yml`:

```
conf/estadisticas_partidos.yml
├── cols_stats_es       → columnas en español
├── cols_stats_en       → columnas en inglés
├── en_fallback         → mapeo ES → EN para coalesce
├── rename_columns      → renombres a snake_case
├── schema              → tipos de destino por columna
├── stats_with_pct      → columnas con formato "valor (pct%)"
├── stats_solo_pct      → columnas con solo "%"
├── goles_from_marcador → parseo del marcador
├── fecha_formats       → formatos de fecha
└── expectations        → reglas de calidad DLT
```

Ventaja: cambiar el schema o agregar columnas no requiere tocar el código Python.
