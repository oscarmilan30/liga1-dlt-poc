# =============================================================================
# UTILITARIOS — Liga1 DLT POC
# Funciones compartidas por Bronze, Silver y Gold.
#
# Uso desde notebooks:   %run ../utils/utils_poc
# Uso desde DLT (.py):   from utils.utils_poc import ...
#
# ── Workspace ─────────────────────────────────────────────────────────────────
#   get_workspace_path()        Ruta absoluta dentro del workspace Databricks
#   get_yaml_config()           Lee conf/estadisticas_partidos.yml
#
# ── Columnas (Bronze, Silver, Gold) ───────────────────────────────────────────
#   rename_columns()            Renombra columnas via diccionario de mapeo
#   sanitize_column_names()     Reemplaza espacios por guiones bajos en nombres
#
# ── Común ─────────────────────────────────────────────────────────────────────
#   log()                       Mensajes con timestamp en los logs del pipeline
# =============================================================================

import os
import yaml
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# -----------------------------------------------------------------------------
# _detect_repo_root
# Detecta la raíz del repo en Databricks.
# Maneja cuatro contextos:
#   1. DLT source-linked → cwd: /Workspace/Repos/.internal/{run_id}/{hash}/...
#   2. Notebooks en Repos → cwd: /Workspace/Repos/{user}/{repo}/...
#   3. DAB bundle deploy  → cwd: /Workspace/Users/{user}/.bundle/{name}/{target}/files/src/...
#   4. Fallback           → retorna cwd tal cual
# -----------------------------------------------------------------------------
def _detect_repo_root() -> str:
    try:
        cwd = os.getcwd()
        if "/Repos/.internal" in cwd:
            # DLT source-linked: cwd incluye run_id y commit_hash
            after = cwd.split("/Repos/.internal/")[1].split("/")
            if len(after) >= 2:
                return f"/Workspace/Repos/.internal/{after[0]}/{after[1]}"
            base = "/Workspace/Repos/.internal/" + after[0]
            subdirs = [d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))]
            return os.path.join(base, subdirs[0]) if subdirs else base
        elif "/Repos/" in cwd:
            # Notebook interactivo en Repos
            parts = cwd.split("/Repos/")[1].split("/")
            return f"/Workspace/Repos/{parts[0]}/{parts[1]}"
        elif "/.bundle/" in cwd and "/files/" in cwd:
            # DAB bundle deploy: .bundle/{bundle}/{target}/files/{src_path}/notebook
            # cwd = .../files/src/bronze → repo_root = .../files
            return cwd.split("/files/")[0] + "/files"
        return cwd
    except Exception:
        return os.getcwd()


# -----------------------------------------------------------------------------
# get_workspace_path
# Construye la ruta absoluta dentro del workspace usando _detect_repo_root().
# Necesaria para leer el YAML desde cualquier contexto (DLT o interactivo).
# -----------------------------------------------------------------------------
def get_workspace_path(relative_path: str) -> str:
    return os.path.join(_detect_repo_root(), relative_path.lstrip("/"))


# -----------------------------------------------------------------------------
# get_yaml_config
# Lee el YAML de configuración y retorna el bloque 'estadisticas_partidos'.
# -----------------------------------------------------------------------------
def get_yaml_config(relative_yaml_path: str = "conf/estadisticas_partidos.yml") -> dict:
    yaml_path = get_workspace_path(relative_yaml_path)
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"No se encontró el YAML en: {yaml_path}")
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["estadisticas_partidos"]


# -----------------------------------------------------------------------------
# rename_columns
# Renombra columnas del DataFrame según un diccionario de mapeo.
# Columnas no incluidas en el dict conservan su nombre original.
#
# Uso: rename_columns(df, {"nombre viejo": "nombre_nuevo", ...})
# -----------------------------------------------------------------------------
def rename_columns(df: DataFrame, rename_dict: dict) -> DataFrame:
    return df.select(
        *[F.col(column).alias(rename_dict.get(column, column)) for column in df.columns]
    )


# -----------------------------------------------------------------------------
# sanitize_column_names
# Reemplaza espacios por guiones bajos en los nombres de columnas.
#
# NECESARIO en Bronze: Delta Lake rechaza columnas con espacios a menos que
# se active Column Mapping. Preferimos renombrar — más portable y predecible.
#
# Internamente usa rename_columns con un dict generado dinámicamente.
# Solo toca columnas que contienen espacios; las demás no cambian.
#
# Ejemplo: "Accurate crosses_local" → "Accurate_crosses_local"
#          "Total shots_visitante"  → "Total_shots_visitante"
# -----------------------------------------------------------------------------
def sanitize_column_names(df: DataFrame) -> DataFrame:
    rename_dict = {c: c.replace(" ", "_") for c in df.columns if " " in c}
    if not rename_dict:
        return df
    return rename_columns(df, rename_dict)




# -----------------------------------------------------------------------------
# log
# Mensajes uniformes con timestamp en los logs del pipeline.
# Niveles: INFO, OK, WARN, ERROR
# -----------------------------------------------------------------------------
def log(msg: str, level: str = "INFO", entity: str = None) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{timestamp}] [{level.upper()}]"
    if entity:
        prefix += f" [{entity}]"
    print(f"{prefix} {msg}")
