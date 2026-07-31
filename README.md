# Liga 1 DLT POC

POC de ingeniería de datos sobre estadísticas históricas de la **Liga 1 Peruana (2020–2026)** usando **Delta Live Tables (Spark Declarative Pipelines)** con arquitectura medallion Bronze → Silver → Gold, desplegado con **Databricks Asset Bundles**.

**Stack:** Python · Azure ADLS Gen2 · Azure Databricks · Delta Live Tables · Unity Catalog · Databricks Asset Bundles · GitHub Actions

---

## ¿Qué demuestra este POC?

| Concepto | Implementación |
|---|---|
| **Spark Declarative Pipelines (DLT)** | Pipeline declarativo con `@dlt.table` — Databricks gestiona orden, reintentos e incremental |
| **Auto Loader** | Ingesta incremental desde ADLS con `cloudFiles` |
| **Data Quality** | Expectations con `@dlt.expect` — validación declarativa en Silver |
| **Schema Evolution** | 7 archivos con esquemas de 5 a 69 columnas manejados automáticamente |
| **Asset Bundles** | Pipeline como código — desplegado con `databricks bundle deploy` |
| **CI/CD dev → prod** | Push a `develop` despliega en dev · Merge a `main` despliega en prod via GitHub Actions |
| **Unity Catalog** | Catálogos `dev_liga1_poc` y `prod_liga1_poc` con schemas bronze/silver/gold |
| **Parametrización YAML** | Schema, renombres, mapeo EN↔ES y expectations definidos en `conf/` |

---

## Arquitectura

```
ADLS Gen2
liga1dltpoc/dlt-poc/landing/estadisticas_partidos/
  7 archivos JSON (2020–2026, ~2,030 partidos)
          │
          │  Auto Loader (cloudFiles)
          ▼
    DLT Pipeline
    ┌─────────────────────────────┐
    │  BRONZE                     │
    │  bronze_estadisticas_       │
    │  partidos (raw)             │
    │           │                 │
    │           ▼ coalesce EN/ES  │
    │  SILVER                     │
    │  silver_partidos            │
    │  (limpio + expectations)    │
    │           │                 │
    │           ▼ aggregaciones   │
    │  GOLD                       │
    │  gold_rendimiento_equipos   │
    │  gold_top_atacantes         │
    │  gold_dominio_historico     │
    └─────────────────────────────┘
          │
          ▼
    Unity Catalog
    dev_liga1_poc / prod_liga1_poc
    ├── bronze
    ├── silver
    └── gold
```

![DLT Pipeline Graph](img/dlt_pipeline_graph.png)

---

## Estructura del repositorio

```
liga1-dlt-poc/
├── databricks.yml                    ← Asset Bundle: targets dev + prod
├── resources/
│   └── liga1_pipeline.yml            ← Definición del pipeline DLT
├── conf/
│   └── estadisticas_partidos.yml     ← Schema, renombres, EN↔ES fallback
├── src/
│   ├── bronze/bronze_partidos.py     ← @dlt.table con Auto Loader
│   ├── silver/silver_partidos.py     ← @dlt.table + @dlt.expect
│   └── gold/gold_partidos.py         ← @dlt.table aggregaciones
├── .github/workflows/
│   ├── deploy-dev.yml                ← Push develop → bundle deploy dev
│   └── deploy-prod.yml               ← Merge main → bundle deploy prod
├── docs/
│   ├── 01_funcional.md               ← Qué hace el proyecto y los datos
│   └── 02_arquitectura.md            ← Infraestructura, permisos y setup
├── img/
│   └── dlt_pipeline_graph.png        ← Captura del grafo DLT
└── datasets/sample/
    └── estadisticas_partidos_2024.json
```

---

## Documentación

| Documento | Contenido |
|---|---|
| [Funcional](docs/01_funcional.md) | Qué hace el proyecto, datos, arquitectura medallion, parametrización |
| [Arquitectura](docs/02_arquitectura.md) | Infraestructura Azure, permisos, Unity Catalog, CLI, Asset Bundles, CI/CD |

---

## Setup rápido

```cmd
# 1. Instalar Databricks CLI
winget install Databricks.DatabricksCLI

# 2. Autenticar
databricks configure

# 3. Desplegar en dev
databricks bundle deploy --target dev

# 4. Correr el pipeline
databricks bundle run liga1_dlt_pipeline --target dev
```

Ver [02_arquitectura.md](docs/02_arquitectura.md) para el setup completo.

---

*Desarrollado por Oscar García Del Águila — Lima, Perú · 2026*
