# Documento de Arquitectura — Liga 1 DLT POC

## Stack tecnológico

| Componente | Tecnología | Propósito |
|---|---|---|
| Almacenamiento | Azure Data Lake Storage Gen2 | Archivos fuente JSON y tablas Delta |
| Cómputo | Azure Databricks (Premium Trial) | Ejecución del pipeline DLT |
| Pipeline | Delta Live Tables (Spark Declarative Pipelines) | Orchestración declarativa Bronze→Silver→Gold |
| Catálogo | Unity Catalog | Gobernanza de datos y namespacing dev/prod |
| Deploy | Databricks Asset Bundles | Pipeline como código, targets dev y prod |
| CI/CD | GitHub Actions | Deploy automático a prod en merge a main |
| Configuración | YAML | Schema, renombres y reglas externalizados |
| Lenguaje | Python 3 + PySpark | Lógica de transformación |

---

## Infraestructura Azure

### Resource Group
- **Nombre:** `rg-liga1`
- **Región:** East US

### Recursos creados

| Recurso | Nombre | Tipo | Notas |
|---|---|---|---|
| Storage Account | `datalakelig1peru` | ADLS Gen2 | Hierarchical Namespace activado |
| Access Connector | `ac-liga1-dlt-poc` | Access Connector for Azure Databricks | Identidad administrada para acceso al ADLS |
| Databricks Workspace | `dbw-liga1-dlt-poc` | Azure Databricks Premium | Trial 14 días — Hybrid mode |

### Permisos configurados

| Quién | Sobre qué | Permiso |
|---|---|---|
| `ac-liga1-dlt-poc` (Managed Identity) | `datalakelig1peru` (Storage Account) | **Storage Blob Data Contributor** |

> Este permiso permite que Databricks lea y escriba en el ADLS sin usar Access Keys.

---

## Estructura ADLS

```
datalakelig1peru  (Storage Account)
└── liga1dltpoc  (Container — dedicado a este POC)
    └── dlt-poc/
        ├── landing/
        │   └── estadisticas_partidos/
        │       ├── estadisticas_partidos_2020.json
        │       ├── estadisticas_partidos_2021.json
        │       ├── estadisticas_partidos_2022.json
        │       ├── estadisticas_partidos_2023.json
        │       ├── estadisticas_partidos_2024.json
        │       ├── estadisticas_partidos_2025.json
        │       └── estadisticas_partidos_2026.json
        └── dev/    ← managed storage del catálogo dev_liga1_poc
```

> **Nota:** Se creó el container `liga1dltpoc` dedicado a este POC para evitar conflictos con el credential del proyecto Liga1 principal (`acc-liga1`). El container anterior `liga1` usa un Access Connector diferente que no se puede modificar por dependencias de file event queue.

---

## Unity Catalog

### Metastore
Compartido con el proyecto Liga1 (un metastore por región en Azure).

### Storage Credential

| Nombre | Tipo | Access Connector | Propósito |
|---|---|---|---|
| `sc-liga1-dlt-poc` | Managed Identity | `ac-liga1-dlt-poc` | Acceso al container `liga1dltpoc` |

### External Location

| Nombre | Credential | URL | Uso |
|---|---|---|---|
| `ext-loc-liga1dltpoc` | `sc-liga1-dlt-poc` | `abfss://liga1dltpoc@datalakelig1peru.dfs.core.windows.net/` | Cubre todo el container `liga1dltpoc` |

> La External Location cubre la raíz del container para permitir tanto la ingesta de datos (landing/) como el managed storage de los catálogos Unity Catalog (dev/, prod/).

### Catálogos

| Catálogo | Storage location | Creación | Propósito |
|---|---|---|---|
| `dev_liga1_poc` | `abfss://liga1dltpoc@datalakelig1peru.dfs.core.windows.net/dlt-poc/dev` | Manual | Ambiente de desarrollo |
| `prod_liga1_poc` | `abfss://liga1dltpoc@datalakelig1peru.dfs.core.windows.net/dlt-poc/prod` | GitHub Actions (Sesión 6) | Ambiente de producción |

### Schemas

```
dev_liga1_poc
├── bronze    ← tablas raw (Auto Loader + partido.*)
├── silver    ← tablas limpias (coalesce EN/ES, expectations)
├── gold      ← aggregaciones de negocio
└── shared    ← infraestructura compartida (Volume con wheel de utils_poc)

prod_liga1_poc  (creado por GitHub Actions)
├── bronze
├── silver
└── gold
```

Creación manual del catálogo dev (ejecutado en notebook):
```sql
CREATE CATALOG dev_liga1_poc
  MANAGED LOCATION 'abfss://liga1dltpoc@datalakelig1peru.dfs.core.windows.net/dlt-poc/dev';

CREATE SCHEMA dev_liga1_poc.bronze;
CREATE SCHEMA dev_liga1_poc.silver;
CREATE SCHEMA dev_liga1_poc.gold;
-- shared se crea automáticamente via tools/build_wheel.py
```

---

## Infraestructura Azure — Producción

### Resource Group
- **Nombre:** `rg-liga1` (compartido con dev) o `rg-liga1-prod` (aislado)
- **Región:** East US

### Recursos prod

| Recurso | Nombre | Tipo | Notas |
|---|---|---|---|
| Storage Account | `datalakelig1prod` | ADLS Gen2 | Hierarchical Namespace activado |
| Access Connector | `ac-liga1-poc-prod` | Access Connector for Azure Databricks | Identidad administrada para acceso al ADLS prod |
| Databricks Workspace | `dbw-liga1-poc-prod` | Azure Databricks Premium | Workspace dedicado a producción |

### Permisos configurados

| Quién | Sobre qué | Permiso |
|---|---|---|
| `ac-liga1-poc-prod` (Managed Identity) | `datalakelig1prod` (Storage Account) | **Storage Blob Data Contributor** |

### Estructura ADLS prod

```
datalakelig1prod  (Storage Account)
└── liga1prod  (Container — dedicado a producción)
    └── dlt-poc/
        ├── landing/
        │   └── estadisticas_partidos/
        │       └── *.json              ← mismos archivos JSON que dev
        └── prod/   ← managed storage del catálogo prod_liga1_poc
```

---

## Unity Catalog — Producción

### Storage Credential prod

| Nombre | Tipo | Access Connector | Propósito |
|---|---|---|---|
| `sc-liga1-poc-prod` | Managed Identity | `ac-liga1-poc-prod` | Acceso al container `liga1prod` |

### External Location prod

| Nombre | Credential | URL | Uso |
|---|---|---|---|
| `ext-loc-liga1prod` | `sc-liga1-poc-prod` | `abfss://liga1prod@datalakelig1prod.dfs.core.windows.net/` | Cubre todo el container `liga1prod` |

### Catálogo prod

| Catálogo | Storage location | Propósito |
|---|---|---|
| `prod_liga1_poc` | `abfss://liga1prod@datalakelig1prod.dfs.core.windows.net/dlt-poc/prod` | Ambiente de producción |

```sql
CREATE CATALOG prod_liga1_poc
  MANAGED LOCATION 'abfss://liga1prod@datalakelig1prod.dfs.core.windows.net/dlt-poc/prod';

CREATE SCHEMA prod_liga1_poc.bronze;
CREATE SCHEMA prod_liga1_poc.silver;
CREATE SCHEMA prod_liga1_poc.gold;
```

> El metastore de Unity Catalog es compartido por región (uno por Azure region).
> El workspace prod se adjunta al mismo metastore que dev — comparten gobierno
> pero los catálogos `dev_liga1_poc` y `prod_liga1_poc` son completamente independientes.

---

## Deploy cross-workspace (Dev → Prod)

### Arquitectura de workspaces

```
Workspace dev  (dbw-liga1-dlt-poc)           Workspace prod  (dbw-liga1-poc-prod)
  catalog: dev_liga1_poc                        catalog: prod_liga1_poc
  ADLS:    datalakelig1peru/liga1dltpoc         ADLS:    datalakelig1prod/liga1prod
  deploy:  manual desde UI                      deploy:  GitHub Actions automático
```

### Variables por target en databricks.yml

| Variable | Dev | Prod |
|---|---|---|
| `catalog` | `dev_liga1_poc` | `prod_liga1_poc` |
| `adls_landing_path` | `abfss://liga1dltpoc@datalakelig1peru...` | `abfss://liga1prod@datalakelig1prod...` |

### Secrets GitHub Actions para prod

| Secret | Descripción |
|---|---|
| `DATABRICKS_HOST_PROD` | URL del workspace prod (`https://adb-xxxx.azuredatabricks.net`) |
| `DATABRICKS_TOKEN_PROD` | Personal Access Token generado en workspace prod |

### Flujo de deploy

```
develop branch  →  deploy manual a dev workspace  (UI Databricks)
      │
      └── Pull Request → main
                │  merge aprobado
                ▼
          GitHub Actions
            └── databricks bundle deploy --target prod
                  └── conecta a dbw-liga1-poc-prod
                        └── crea/actualiza pipeline en workspace prod
```

---

## Databricks CLI

### Instalación (Windows)

```cmd
winget install Databricks.DatabricksCLI
```

Versión usada: `v1.9.0`

### Autenticación

```cmd
databricks configure
# Host:  https://<workspace-id>.azuredatabricks.net
# Token: <Personal Access Token generado en Databricks Settings>
```

**Token:** generado en Databricks → Settings → Developer → Access tokens
- Name: `cli-local`
- Lifetime: 90 días
- Scope: `all-apis`

### Verificación

```cmd
databricks workspace list /
```

---

## Cómo funciona utils_poc en DLT

### ¿Por qué existe?

`utils_poc.py` contiene funciones compartidas por Bronze, Silver y Gold: `get_yaml_config`, `sanitize_column_names`, `rename_columns` y `log`. Vivir en un solo archivo evita duplicar código en tres notebooks.

### El patrón `file:` + bootstrap

DLT ejecuta cada notebook en su **propio scope Python** — `file:` no comparte namespace automáticamente entre archivos. Por eso cada notebook necesita un bloque de bootstrap que añade `src/` al `sys.path` y luego importa explícitamente de `utils_poc`.

El bootstrap detecta la raíz del repo en dos contextos:
- **Pipeline normal:** `/Workspace/Repos/{user}/{repo}/`
- **DLT source-linked:** `/Workspace/Repos/.internal/{run_id}/{hash}/`

```python
# 5 líneas iguales en bronze, silver y gold
_cwd = os.getcwd()
if "/Repos/.internal/" in _cwd:
    _p = _cwd.split("/Repos/.internal/")[1].split("/")
    _repo_root = f"/Workspace/Repos/.internal/{_p[0]}/{_p[1]}"
elif "/Repos/" in _cwd:
    _p = _cwd.split("/Repos/")[1].split("/")
    _repo_root = f"/Workspace/Repos/{_p[0]}/{_p[1]}"
else:
    _repo_root = _cwd
if os.path.join(_repo_root, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_repo_root, "src"))

from utils.utils_poc import get_yaml_config, log  # import explícito
```

`utils_poc.py` sigue declarado como `file:` en el pipeline porque DLT lo valida y lo incluye en el bundle — pero el acceso real a sus funciones es vía `sys.path` + `import`.

### Cuándo actualizar utils_poc.py

Modificar el archivo en el repo → hacer `bundle deploy` → lanzar el pipeline. No hay pasos intermedios de build ni upload.

---

## Asset Bundles

### Variables

El `databricks.yml` declara todas las variables reutilizables:

| Variable | Dev | Prod | Descripción |
|---|---|---|---|
| `catalog` | `dev_liga1_poc` | `prod_liga1_poc` | Catálogo Unity Catalog destino |
| `adls_landing_path` | `abfss://liga1dltpoc@.../landing/estadisticas_partidos` | igual | Ruta ADLS de los JSON fuente |
| `schema_bronze` | `bronze` | `bronze` | Schema de la capa Bronze |
| `schema_silver` | `silver` | `silver` | Schema de la capa Silver |
| `schema_gold` | `gold` | `gold` | Schema de la capa Gold |

### Pipeline libraries — `file:` y `notebook:`

| Archivo | Tipo en YAML | Razón |
|---|---|---|
| `utils_poc.py` | `file:` | Módulo utilitario ejecutado primero — funciones en namespace global |
| `bronze_partidos.py` | `notebook:` | Notebook con celdas markdown + `@dlt.table` |
| `silver_partidos.py` | `notebook:` | Notebook con celdas markdown + `@dlt.table` |
| `gold_partidos.py` | `notebook:` | Notebook con celdas markdown + `@dlt.table` |

DLT no comparte namespace entre archivos — cada notebook importa explícitamente via bootstrap + `from utils.utils_poc import ...`.

### `target` del pipeline

DLT con Unity Catalog requiere un schema por defecto (`target`). Se parametriza con la variable `schema_bronze`:

```yaml
catalog: ${var.catalog}
target: ${var.schema_bronze}   # fallback obligatorio de Unity Catalog
```

Cada `@dlt.table` sobreescribe el `target` con su propio schema leído via `spark.conf.get()`.

### Comandos principales

```cmd
# Desplegar en dev
databricks bundle deploy --target dev

# Correr el pipeline en dev
databricks bundle run liga1_dlt_pipeline --target dev

# Validar el bundle sin desplegar
databricks bundle validate
```

### Deploy desde UI Databricks

Alternativa al CLI — útil durante desarrollo:
1. Abrir cualquier archivo del bundle en Workspace
2. Clic en banner "This file is part of a bundle"
3. Panel Deployments → **Deploy**

---

## CI/CD — GitHub Actions

### Ramas

| Rama | Ambiente | Deploy |
|---|---|---|
| `develop` | dev | Manual (UI o `databricks bundle deploy --target dev`) |
| `main` | prod | Automático via GitHub Actions |

### Flujo

```
develop branch
    │  push / trabajo diario
    │  deploy manual desde Databricks UI o CLI
    ▼
Pull Request develop → main
    │  merge aprobado
    ▼
.github/workflows/deploy-prod.yml
    └── databricks bundle deploy --target prod
```

### Secrets en GitHub

| Secret | Valor |
|---|---|
| `DATABRICKS_HOST` | URL del workspace |
| `DATABRICKS_TOKEN` | Personal Access Token |

> Configurar en: GitHub repo → Settings → Secrets and variables → Actions

---

## Explicación del `databricks.yml`

### `bundle`
Nombre del proyecto Bundle. Databricks lo usa para identificar todos los recursos desplegados y aparece en la UI.

### `variables`
Variables reutilizables con valores por defecto. Evitan rutas y nombres hardcodeados. Se referencian con `${var.nombre}` en cualquier archivo del Bundle.

### `targets`
Define los ambientes dev y prod:
- `default: true` → si no especificas target, usa dev
- `mode: development` → pipeline en modo económico, sin retention
- `mode: production` → optimizaciones de prod con retention
- Cada target sobreescribe solo las variables que cambian (en este caso `catalog`)

### `configuration`
Spark configurations inyectadas al pipeline en runtime. El código Python las lee con:
```python
ADLS_LANDING_PATH = spark.conf.get("adls_landing_path")
SCHEMA_BRONZE     = spark.conf.get("schema_bronze")
```
Es el puente entre el YAML y el código — sin valores hardcodeados.

### `libraries`
Archivos con las definiciones `@dlt.table` y utilitarios. El Bundle los registra en el pipeline al hacer deploy. `utils_poc.py` va primero como `file:` para que sus funciones estén disponibles globalmente.

### `clusters`
Cluster que DLT levanta para ejecutar el pipeline:
- `num_workers: 0` + `singleNode` → nodo único, mínimo costo
- `Standard_D4s_v3` → VM con 16 GB RAM en Azure
- Se destruye automáticamente al terminar el pipeline

---

## Separación de responsabilidades — capas y funciones

Regla de oro: **si una función solo la usa una capa, vive en esa capa. Si la usan dos o más capas, vive en `utils_poc.py`.**

| Función | Dónde vive | Razón |
|---|---|---|
| `get_yaml_config()` | `utils_poc.py` | Bronze, Silver y Gold la leen |
| `sanitize_column_names()` | `utils_poc.py` | Silver la usa; podría reutilizarla Gold |
| `rename_columns()` | `utils_poc.py` | Base de `sanitize_column_names`, reutilizable |
| `log()` | `utils_poc.py` | Todos los notebooks logean |
| `_detect_repo_root()` | `utils_poc.py` | Interna de `get_yaml_config` |
| `_read_landing()` | `bronze_partidos.py` | Solo Bronze lee del ADLS con Auto Loader |
| `_explode()` | `bronze_partidos.py` | Solo Bronze desanida el JSON |
| `_add_timestamp()` | `bronze_partidos.py` | Auditoría de ingesta, solo Bronze |
| `_coalesce_es_en()` | `silver_partidos.py` | Solo Silver unifica ES/EN |
| `_parse_fecha()` | `silver_partidos.py` | Solo Silver parsea fechas |
| `_parse_marcador()` | `silver_partidos.py` | Solo Silver extrae goles y resultado |
| `_split_valor_pct()` | `silver_partidos.py` | Solo Silver separa valor y porcentaje |
