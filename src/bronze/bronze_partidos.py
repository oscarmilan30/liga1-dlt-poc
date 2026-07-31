# Databricks notebook source
# MAGIC %md
# MAGIC # CAPA BRONZE — bronze_partidos
# MAGIC
# MAGIC **Responsabilidad:** Ingestar los archivos JSON desde ADLS usando Auto Loader.
# MAGIC No transforma datos. Solo lee, registra la fuente y cuándo llegó.
# MAGIC Es la "foto fiel" de los datos originales.
# MAGIC
# MAGIC **Flujo:**
# MAGIC ```
# MAGIC ADLS (landing/*.json)
# MAGIC   └─► _read_landing()      Auto Loader streaming, schema inferido
# MAGIC         └─► _explode()     desanida array "data" → filas individuales
# MAGIC               └─► _add_timestamp()  agrega _load_timestamp
# MAGIC                     └─► bronze_partidos (tabla Delta, Column Mapping)
# MAGIC ```
# MAGIC
# MAGIC **Column Mapping:** habilitado (`delta.columnMapping.mode = name`) para que Delta
# MAGIC acepte los nombres originales del JSON, incluidos los que tienen espacios
# MAGIC (ej: `"Accurate crosses_local"`). Silver se encarga de sanitizar y renombrar.
# MAGIC
# MAGIC **Tabla destino:**
# MAGIC - Dev:  `dev_liga1_poc.bronze.bronze_partidos`
# MAGIC - Prod: `prod_liga1_poc.bronze.bronze_partidos`

# COMMAND ----------

import dlt
import sys
import os
from pyspark.sql import SparkSession, functions as F

# ── Bootstrap: añadir src/ al sys.path para importar utils_poc ───────────────
# DLT ejecuta cada notebook en su propio scope — file: no comparte namespace.
# Estas 5 líneas detectan la raíz del repo en cualquier contexto DLT:
#   - Pipeline normal:       /Workspace/Repos/{user}/{repo}/
#   - DLT source-linked:     /Workspace/Repos/.internal/{run_id}/{hash}/
# _detect_repo_root() vive en utils_poc pero aún no podemos importarla,
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

from utils.utils_poc import get_yaml_config, log

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

# Ruta ADLS donde viven los JSON de entrada
ADLS_LANDING_PATH = spark.conf.get("adls_landing_path")

# Schema de destino (para logging — el pipeline YAML controla dónde se escribe)
SCHEMA_BRONZE = spark.conf.get("schema_bronze")

# Configuración del YAML: campo_json, columnas, etc.
CONFIG = get_yaml_config("conf/estadisticas_partidos.yml")

log(f"Landing path: {ADLS_LANDING_PATH}", entity="bronze_partidos")
log(f"Schema destino: {SCHEMA_BRONZE}", entity="bronze_partidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones Bronze
# MAGIC
# MAGIC Estas funciones son **específicas de Bronze** — leen desde ADLS y preparan
# MAGIC la estructura para escribir a Delta. No se reutilizan en Silver ni Gold,
# MAGIC por eso viven aquí y no en `utils_poc.py`.

# COMMAND ----------

def _read_landing(adls_path: str) -> "DataFrame":
    """
    Lee los JSON del ADLS de forma incremental con Auto Loader (cloudFiles).

    cloudFiles.schemaLocation: carpeta donde Auto Loader persiste el schema
    inferido entre runs. Necesario para streaming incremental.

    inferColumnTypes: infiere int/double/string en lugar de dejar todo string.
    Permite detectar automáticamente columnas nuevas al agregar archivos.
    """
    spark = SparkSession.getActiveSession()
    schema_location = f"{adls_path}/_schema_checkpoint/bronze_partidos"
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(adls_path)
    )


def _explode(df: "DataFrame", campo_json: str) -> "DataFrame":
    """
    Desanida el array del JSON en filas individuales.

    El JSON tiene estructura: { "temporada": 2021, "data": [ {...partido1...}, ... ] }
    Esta función:
      PASO 1: extrae temporada + _source_file + explode(data) → una fila por partido
      PASO 2: expande partido.* → todas las columnas del struct como columnas planas

    _source_file se captura ANTES del explode porque _metadata solo existe
    en el DataFrame crudo de Auto Loader (desaparece después del explode).

    Bronze captura TODOS los campos del JSON (partido.*) — no filtra por YAML.
    Cada año tiene un schema diferente; Auto Loader unifica con nulls.
    """
    df_exploded = df.select(
        F.col("temporada"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.explode(F.col(campo_json)).alias("partido")
    )
    return df_exploded.select(
        F.col("temporada"),
        F.col("_source_file"),
        F.col("partido.*")
    )


def _add_timestamp(df: "DataFrame") -> "DataFrame":
    """
    Agrega _load_timestamp: cuándo llegó el registro al pipeline.
    Columna de auditoría — permite rastrear el momento de ingesta.
    """
    return df.select(
        *[F.col(c) for c in df.columns],
        F.current_timestamp().alias("_load_timestamp")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Bronze

# COMMAND ----------

@dlt.table(
    name=f"{SCHEMA_BRONZE}.bronze_partidos",
    comment=(
        "Ingesta raw de estadísticas de partidos Liga1 Peruana 2020-2026. "
        "Datos sin transformar desde ADLS via Auto Loader. "
        "Column Mapping habilitado para conservar nombres originales del JSON con espacios."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        # Column Mapping: permite nombres de columna con espacios (ej: "Accurate crosses_local").
        # Bronze no transforma — Silver aplica sanitize + rename + coalesce ES/EN.
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    }
)
def bronze_partidos():
    df = _read_landing(ADLS_LANDING_PATH)
    df = _explode(df, CONFIG["campo_json"])
    df = _add_timestamp(df)
    return df
