# Databricks notebook source
# MAGIC %md
# MAGIC # CAPA GOLD — gold_partidos
# MAGIC
# MAGIC **Responsabilidad:** Agregaciones de negocio sobre Silver. Tablas listas para
# MAGIC consumo directo en dashboards o análisis — sin JOINs adicionales.
# MAGIC
# MAGIC **Tablas:**
# MAGIC ```
# MAGIC silver_partidos
# MAGIC   ├─► gold_tabla_posiciones    Standings por temporada y equipo (2020-2026)
# MAGIC   └─► gold_rendimiento_equipo  Stats promedio local/visitante por equipo (2022+)
# MAGIC ```
# MAGIC
# MAGIC **¿Por qué no hay dimensiones ni fact tables?**
# MAGIC Silver ya es la tabla de hechos limpia. Gold es la capa de consumo —
# MAGIC tablas pre-agregadas y denormalizadas. El analista no hace JOINs: ya viene
# MAGIC todo calculado.
# MAGIC
# MAGIC **Tabla destino:**
# MAGIC - Dev:  `dev_liga1_poc.gold.*`
# MAGIC - Prod: `prod_liga1_poc.gold.*`

# COMMAND ----------

import dlt
import sys
import os
from pyspark.sql import functions as F
from pyspark.sql import Window

# ── Bootstrap: añadir src/ al sys.path para importar utils_poc ───────────────
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

SCHEMA_GOLD   = spark.conf.get("schema_gold")
SCHEMA_SILVER = spark.conf.get("schema_silver")
CONFIG        = get_yaml_config("conf/estadisticas_partidos.yml")

log(f"Schema destino: {SCHEMA_GOLD}", entity="gold_partidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones Gold
# MAGIC
# MAGIC **`_unpivot_equipo(df)`** — transforma cada partido (1 fila) en 2 filas:
# MAGIC una por equipo participante. Invierte goles y resultado para el visitante
# MAGIC para que todas las métricas queden en perspectiva del equipo (no del local).
# MAGIC
# MAGIC ```
# MAGIC Partido: Melgar 2 - 1 Universitario   resultado="L"
# MAGIC   → fila Melgar:        goles_favor=2, goles_contra=1, es_victoria=1
# MAGIC   → fila Universitario: goles_favor=1, goles_contra=2, es_victoria=0
# MAGIC ```

# COMMAND ----------

def _unpivot_equipo(df, stat_cols):
    """
    Una fila por partido → dos filas: una para local, otra para visitante.
    Invierte goles y resultado para el visitante para perspectiva del equipo.

    resultado en Silver está en perspectiva del local:
      "L" = local ganó, "V" = visitante ganó, "E" = empate

    Para la fila local:     L=victoria, V=derrota, E=empate
    Para la fila visitante: V=victoria, L=derrota, E=empate

    stat_cols: lista de columnas base (sin sufijo) leída del YAML.
    """
    base_local = [
        F.col("temporada"),
        F.col("equipo_local").alias("equipo"),
        F.lit("local").alias("rol"),
        F.col("goles_local").alias("goles_favor"),
        F.col("goles_visitante").alias("goles_contra"),
        F.when(F.col("resultado") == "L", 1).otherwise(0).alias("es_victoria"),
        F.when(F.col("resultado") == "E", 1).otherwise(0).alias("es_empate"),
        F.when(F.col("resultado") == "V", 1).otherwise(0).alias("es_derrota"),
    ]
    base_visitante = [
        F.col("temporada"),
        F.col("equipo_visitante").alias("equipo"),
        F.lit("visitante").alias("rol"),
        F.col("goles_visitante").alias("goles_favor"),
        F.col("goles_local").alias("goles_contra"),
        F.when(F.col("resultado") == "V", 1).otherwise(0).alias("es_victoria"),
        F.when(F.col("resultado") == "E", 1).otherwise(0).alias("es_empate"),
        F.when(F.col("resultado") == "L", 1).otherwise(0).alias("es_derrota"),
    ]

    # Stats desde YAML: _local para la fila local, _visitante para la fila visitante
    stats_local     = [F.col(f"{c}_local").alias(c)     for c in stat_cols]
    stats_visitante = [F.col(f"{c}_visitante").alias(c) for c in stat_cols]

    local_df     = df.select(*base_local,     *stats_local)
    visitante_df = df.select(*base_visitante, *stats_visitante)
    return local_df.union(visitante_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_tabla_posiciones
# MAGIC
# MAGIC Tabla de posiciones acumulada por temporada y equipo (2020-2026).
# MAGIC Usa solo columnas del Grupo 1 — siempre disponibles en todos los años.
# MAGIC
# MAGIC **Puntos:** Victoria=3, Empate=1, Derrota=0
# MAGIC **Desempate:** puntos → diferencia de goles → goles a favor
# MAGIC **posicion:** rank() dentro de cada temporada (puede repetirse en empate exacto)

# COMMAND ----------


_exp_tabla_posiciones = {
    e["name"]: e["constraint"]
    for e in CONFIG["gold_expectations_tabla_posiciones"]
}


@dlt.table(
    name=f"{SCHEMA_GOLD}.gold_tabla_posiciones",
    comment=(
        "Tabla de posiciones Liga1 Peruana 2020-2026 por temporada y equipo. "
        "PJ, V, E, D, GF, GC, DG, Pts y posición calculados desde Silver. "
        "Excluye partidos sin jugar (resultado IS NULL)."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_all(_exp_tabla_posiciones)
def gold_tabla_posiciones():
    log("Construyendo gold_tabla_posiciones...", entity="gold_partidos")

    stat_cols = CONFIG["gold_stats_cols"]

    # Solo partidos jugados — excluye "Sin jugar" (resultado null)
    # Nombre calificado con schema porque silver ≠ target del pipeline (bronze)
    df = dlt.read(f"{SCHEMA_SILVER}.silver_partidos").filter(F.col("resultado").isNotNull())

    # Una fila por equipo por partido
    df = _unpivot_equipo(df, stat_cols)

    # Agregación por temporada + equipo (sin rol — acumula local+visitante)
    agg = df.groupBy("temporada", "equipo").agg(
        F.count("*").alias("partidos_jugados"),
        F.sum("es_victoria").cast("int").alias("victorias"),
        F.sum("es_empate").cast("int").alias("empates"),
        F.sum("es_derrota").cast("int").alias("derrotas"),
        F.sum("goles_favor").alias("goles_favor"),
        F.sum("goles_contra").alias("goles_contra"),
        (F.sum("goles_favor") - F.sum("goles_contra")).alias("diferencia_goles"),
        (F.sum("es_victoria") * 3 + F.sum("es_empate")).cast("int").alias("puntos"),
    )

    # Posición dentro de cada temporada — select en lugar de withColumn
    window = Window.partitionBy("temporada").orderBy(
        F.col("puntos").desc(),
        F.col("diferencia_goles").desc(),
        F.col("goles_favor").desc(),
    )
    result = agg.select(
        *[F.col(c) for c in agg.columns],
        F.rank().over(window).alias("posicion"),
    )

    log("gold_tabla_posiciones OK", entity="gold_partidos")
    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_rendimiento_equipo
# MAGIC
# MAGIC Rendimiento promedio por equipo, temporada y rol (local/visitante).
# MAGIC Filtrado a **2022+** — antes no hay estadísticas detalladas en los datos fuente.
# MAGIC
# MAGIC `avg()` ignora nulls automáticamente: si un partido no tiene stats,
# MAGIC no distorsiona el promedio del equipo.

# COMMAND ----------


_exp_rendimiento_equipo = {
    e["name"]: e["constraint"]
    for e in CONFIG["gold_expectations_rendimiento_equipo"]
}


@dlt.table(
    name=f"{SCHEMA_GOLD}.gold_rendimiento_equipo",
    comment=(
        "Rendimiento promedio por equipo, temporada y rol (local/visitante). "
        "Solo temporadas 2022-2026 donde existen estadísticas detalladas. "
        "Métricas: posesión, tiros, disparos a puerta, faltas, tarjetas, esquinas."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_all(_exp_rendimiento_equipo)
def gold_rendimiento_equipo():
    log("Construyendo gold_rendimiento_equipo...", entity="gold_partidos")

    stat_cols   = CONFIG["gold_stats_cols"]
    anio_inicio = CONFIG["gold_rendimiento_anio_inicio"]

    # Partidos jugados desde el año configurado en YAML — años con stats detalladas
    df = dlt.read(f"{SCHEMA_SILVER}.silver_partidos").filter(
        F.col("resultado").isNotNull() & (F.col("temporada") >= anio_inicio)
    )

    # Una fila por equipo por partido, con stats de ese equipo
    df = _unpivot_equipo(df, stat_cols)

    # Agregación por temporada + equipo + rol (local vs visitante por separado)
    result = df.groupBy("temporada", "equipo", "rol").agg(
        F.count("*").alias("partidos_jugados"),
        F.sum("es_victoria").cast("int").alias("victorias"),
        F.sum("es_empate").cast("int").alias("empates"),
        F.sum("es_derrota").cast("int").alias("derrotas"),
        F.sum("goles_favor").alias("goles_favor"),
        F.sum("goles_contra").alias("goles_contra"),
        (F.sum("es_victoria") * 3 + F.sum("es_empate")).cast("int").alias("puntos"),
        # Promedios — avg() excluye nulls, no distorsiona cuando faltan stats
        F.round(F.avg("posesion"),           1).alias("posesion_promedio"),
        F.round(F.avg("tiros_totales"),      1).alias("tiros_totales_promedio"),
        F.round(F.avg("disparos_puerta"),    1).alias("disparos_puerta_promedio"),
        F.round(F.avg("faltas"),             1).alias("faltas_promedio"),
        F.round(F.avg("tarjetas_amarillas"), 2).alias("tarjetas_amarillas_promedio"),
        F.round(F.avg("tarjetas_rojas"),     2).alias("tarjetas_rojas_promedio"),
        F.round(F.avg("saques_esquina"),     1).alias("saques_esquina_promedio"),
    )

    log("gold_rendimiento_equipo OK", entity="gold_partidos")
    return result
