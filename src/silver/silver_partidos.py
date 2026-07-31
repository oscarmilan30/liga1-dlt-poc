# Databricks notebook source
# MAGIC %md
# MAGIC # CAPA SILVER — silver_partidos
# MAGIC
# MAGIC **Responsabilidad:** Limpiar y estandarizar los datos de Bronze.
# MAGIC Bronze guarda todos los campos del JSON tal como vienen (con Column Mapping).
# MAGIC Silver aplica todas las transformaciones y produce columnas canónicas.
# MAGIC
# MAGIC **Flujo de transformaciones:**
# MAGIC ```
# MAGIC bronze_partidos  (columnas originales con espacios via Column Mapping)
# MAGIC   └─► sanitize_column_names()   espacios → guiones bajos
# MAGIC         └─► coalesce_es_en()    unifica ES/EN → snake_case (YAML)
# MAGIC               └─► parse_fecha() fecha_raw string → fecha date
# MAGIC                     └─► parse_marcador()  marcador → goles + resultado
# MAGIC                           └─► split_valor_pct()  "15 (75%)" → val + pct
# MAGIC                                 └─► silver_partidos (tabla Delta)
# MAGIC ```
# MAGIC
# MAGIC **¿Por qué coalesce ES/EN?**
# MAGIC - 2020: solo columnas base (sin estadísticas)
# MAGIC - 2021: estadísticas en español ("Tiros totales_local")
# MAGIC - 2022-2026: estadísticas en inglés ("Total shots_local")
# MAGIC Bronze captura ambas como columnas separadas (null para el idioma ausente).
# MAGIC Silver hace `coalesce(ES, EN)` para unificar en una sola columna snake_case.
# MAGIC
# MAGIC **Tabla destino:**
# MAGIC - Dev:  `dev_liga1_poc.silver.silver_partidos`
# MAGIC - Prod: `prod_liga1_poc.silver.silver_partidos`

# COMMAND ----------

import dlt
import sys
import os
from pyspark.sql import functions as F

# ── Bootstrap: añadir src/ al sys.path para importar utils_poc ───────────────
# DLT ejecuta cada notebook en su propio scope — file: no comparte namespace.
# Misma lógica inline que bronze_partidos.py — detecta raíz del repo en cualquier
# contexto DLT (pipeline normal o source-linked con .internal/).
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

from utils.utils_poc import get_yaml_config, rename_columns, sanitize_column_names, log

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones Silver
# MAGIC
# MAGIC Estas funciones son **específicas de Silver** — transforman los datos de Bronze
# MAGIC al esquema canónico. No se reutilizan en Bronze ni Gold, por eso viven aquí
# MAGIC y no en `utils_poc.py`.

# COMMAND ----------

def _coalesce_es_en(df, config):
    """
    Unifica columnas ES/EN → snake_case usando el YAML.

    Bronze guarda ambas columnas (null para el idioma ausente):
      - 2021 ES: "Tiros_totales_local"=15,  "Total_shots_local"=null
      - 2022 EN: "Tiros_totales_local"=null, "Total_shots_local"=15

    Lee en_fallback (mapeo ES→EN) y rename_columns (mapeo ES→snake_case)
    del YAML y genera: coalesce(ES_san, EN_san).alias(snake_case).
    """
    en_fallback = config["en_fallback"]
    rename_map  = config["rename_columns"]

    base_exprs = [
        F.col("temporada").cast("int"),
        F.col("fecha").alias("fecha_raw"),
        F.col("local").alias("equipo_local"),
        F.col("visitante").alias("equipo_visitante"),
        F.col("marcador"),
        F.col("url"),
        F.col("_source_file").alias("fuente"),
        F.col("_load_timestamp").alias("fecha_carga"),
    ]

    stats_exprs = []
    seen = set()
    for es_col, en_col in en_fallback.items():
        target = rename_map.get(es_col)
        if not target or target in seen:
            continue
        seen.add(target)
        es_san = es_col.replace(" ", "_")
        en_san = en_col.replace(" ", "_")
        if es_san == en_san:
            stats_exprs.append(F.col(es_san).alias(target))
        else:
            stats_exprs.append(F.coalesce(F.col(es_san), F.col(en_san)).alias(target))

    return df.select(*base_exprs, *stats_exprs)


def _parse_fecha(df, config):
    """
    Parsea fecha_raw (string) → fecha (DateType).

    Spark 3.0+ no soporta EEEE (día de la semana) en DateTimeFormatter.
    Solución: stripear el día con regex antes de llamar to_date().

    Tres formatos en cascada:
      1. "25-07-2021"            → to_date(fecha_raw, "dd-MM-yyyy")
      2. "Sunday, July 25, 2021" → strip "Sunday, " → "July 25, 2021"
                                 → to_date("MMMM d, yyyy")
      3. "Sunday, July 20"       → strip "Sunday, " → "July 20"
                                 → concat temporada → "July 20, 2026"
                                 → to_date("MMMM d, yyyy")

    fecha_raw se elimina del output (Silver solo expone fecha parseada).
    """
    fmts = config["fecha_formats"]

    # Elimina el día de la semana y la coma del inicio: "Sunday, " → ""
    fecha_sin_dia = F.regexp_replace(F.col("fecha_raw"), r"^[A-Za-z]+,\s*", "")

    fecha_col = F.coalesce(
        F.to_date(F.col("fecha_raw"), fmts["formato_numerico"]),
        F.to_date(fecha_sin_dia, fmts["formato_texto_en"]),
        F.to_date(
            F.concat(fecha_sin_dia, F.lit(", "), F.col("temporada").cast("string")),
            fmts["formato_texto_en"]
        )
    ).alias("fecha")
    other_cols = [F.col(c) for c in df.columns if c != "fecha_raw"]
    return df.select(*other_cols, fecha_col)


def _parse_marcador(df):
    """
    Extrae goles y resultado desde la columna marcador.

    "2 - 1" → goles_local=2, goles_visitante=1, resultado="L"
    "Sin jugar" → goles=null, resultado=null

    resultado: "L"=local gana, "V"=visitante gana, "E"=empate, null=sin jugar.
    """
    goles_local     = F.split(F.col("marcador"), r" - ").getItem(0).cast("int").alias("goles_local")
    goles_visitante = F.split(F.col("marcador"), r" - ").getItem(1).cast("int").alias("goles_visitante")
    df2 = df.select(*[F.col(c) for c in df.columns], goles_local, goles_visitante)

    resultado = (
        F.when(F.col("goles_local") >  F.col("goles_visitante"), "L")
         .when(F.col("goles_local") <  F.col("goles_visitante"), "V")
         .when(F.col("goles_local") == F.col("goles_visitante"), "E")
         .otherwise(None).alias("resultado")
    )
    return df2.select(*[F.col(c) for c in df2.columns], resultado)


def _split_valor_pct(df, config):
    """
    Divide columnas "340 (80%)" → valor int + pct double.

    stats_with_pct (YAML): "340 (80%)" → col=340, col_pct=80.0
    stats_solo_pct (YAML): "45%"       → col=45  (posesion)

    cast("string") antes de regexp_extract maneja columnas inferidas
    como int o string por Auto Loader.
    """
    stats_with_pct = config.get("stats_with_pct", [])
    stats_solo_pct = config.get("stats_solo_pct", [])
    existing    = set(df.columns)
    drop_cols   = set()
    extra_exprs = []

    for col_name in stats_with_pct:
        if col_name not in existing:
            continue
        src = F.col(col_name).cast("string")
        extra_exprs.append(F.regexp_extract(src, r"^(\d+)", 1).cast("int").alias(col_name))
        extra_exprs.append(F.regexp_extract(src, r"\((\d+(?:\.\d+)?)%\)", 1).cast("double").alias(f"{col_name}_pct"))
        drop_cols.add(col_name)

    for col_name in stats_solo_pct:
        if col_name not in existing:
            continue
        src = F.col(col_name).cast("string")
        extra_exprs.append(F.regexp_extract(src, r"(\d+(?:\.\d+)?)", 1).cast("int").alias(col_name))
        drop_cols.add(col_name)

    keep_cols = [F.col(c) for c in df.columns if c not in drop_cols]
    return df.select(*keep_cols, *extra_exprs)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

# Schema de destino — inyectado por el pipeline via spark.conf
SCHEMA_SILVER = spark.conf.get("schema_silver")

# Configuración del YAML: rename_columns, en_fallback, expectations, etc.
CONFIG = get_yaml_config("conf/estadisticas_partidos.yml")

log(f"Schema destino: {SCHEMA_SILVER}", entity="silver_partidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Silver
# MAGIC
# MAGIC ### Expectations (Data Quality)
# MAGIC Las expectations se leen del YAML (`expectations`) para mantener
# MAGIC las reglas de calidad externalizadas y sin hardcoding.
# MAGIC
# MAGIC `@dlt.expect_all` registra violaciones como métricas en el pipeline
# MAGIC sin descartar filas — permite analizar la calidad antes de ser más estrictos.
# MAGIC
# MAGIC ### Orden de transformaciones
# MAGIC 1. `sanitize_column_names()` — espacios → guiones bajos (Bronze usa Column Mapping)
# MAGIC 2. `coalesce_es_en()` — unifica columnas ES/EN del YAML → snake_case
# MAGIC 3. `parse_fecha()` — fecha_raw string → fecha date (3 formatos posibles)
# MAGIC 4. `parse_marcador()` — "2 - 1" → goles_local=2, goles_visitante=1, resultado="L"
# MAGIC 5. `split_valor_pct()` — "340 (80%)" → valor=340 int, pct=80.0 double

# COMMAND ----------

# Expectations desde YAML → dict requerido por @dlt.expect_all
_expectations = {
    exp["name"]: exp["constraint"]
    for exp in CONFIG["expectations"]
}


@dlt.table(
    name=f"{SCHEMA_SILVER}.silver_partidos",
    comment=(
        "Estadísticas de partidos Liga1 Peruana 2020-2026. "
        "Columnas unificadas ES/EN en snake_case, fechas parseadas, "
        "goles extraídos del marcador, stats valor/pct separadas."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_all(_expectations)
def silver_partidos():
    log("Leyendo bronze_partidos...", entity="silver_partidos")

    # 1. Leer Bronze — dlt.read() crea una lectura streaming de la tabla Delta
    df = dlt.read("bronze_partidos")

    # 2. Sanitize: "Accurate crosses_local" → "Accurate_crosses_local"
    #    Bronze usa Column Mapping para guardar nombres con espacios.
    #    Silver necesita referenciar esas columnas → sanitizamos primero.
    df = sanitize_column_names(df)

    # 3. Coalesce ES/EN → snake_case final
    #    coalesce("Tiros_totales_local", "Total_shots_local").alias("tiros_totales_local")
    #    Columnas del YAML (en_fallback + rename_columns) controlan el mapeo.
    df = _coalesce_es_en(df, CONFIG)

    # 4. Parsear fecha: "25-07-2021" / "Sunday, July 25, 2021" / "Sunday, July 20"
    #    → fecha (DateType). fecha_raw se elimina del output.
    df = _parse_fecha(df, CONFIG)

    # 5. Parsear marcador: "2 - 1" → goles_local=2, goles_visitante=1, resultado="L"
    #    "Sin jugar" → goles=null, resultado=null
    df = _parse_marcador(df)

    # 6. Dividir columnas "valor (pct%)": "340 (80%)" → valor=340, pct=80.0
    #    stats_with_pct y stats_solo_pct definidos en el YAML.
    df = _split_valor_pct(df, CONFIG)

    log(f"Silver OK — {len(df.columns)} columnas", entity="silver_partidos")
    return df
