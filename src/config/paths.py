"""Rutas del proyecto centralizadas con pathlib."""
from pathlib import Path

# Raíz del proyecto (3 niveles arriba: src/config/paths.py -> raíz)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Directorios de datos
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
LAKE_DIR = DATA_DIR / "lake"

# Rutas listas para usar (en formato POSIX para compatibilidad total con Spark en Windows)
BRONZE_PATH = (LAKE_DIR / "bronze").as_posix()
BRONZE_PATH_SENTIMIENTO = (LAKE_DIR / "bronze" / "sentimiento").as_posix()
BRONZE_PATH_IBEX = (LAKE_DIR / "bronze" / "ibex").as_posix()
SILVER_PATH = (LAKE_DIR / "silver").as_posix()
GOLD_PATH = (LAKE_DIR / "gold").as_posix()
COMPANIES_JSON_PATH = RAW_DATA_DIR / "companies.json"
SENTIMIENTO_CSV_PATH = (RAW_DATA_DIR / "sentimiento_ibex.csv").as_posix()
SILVER_PATH_SENTIMIENTO = (LAKE_DIR / "silver" / "sentimiento").as_posix()
BRONZE_PATH_EMPRESA = (LAKE_DIR / "bronze" / "empresa").as_posix()
SILVER_PATH_EMPRESA = (LAKE_DIR / "silver" / "empresa").as_posix()
SILVER_PATH_IBEX = (LAKE_DIR / "silver" / "ibex").as_posix()

GOLD_PATH_HECHO = (LAKE_DIR / "gold" / "hecho_cotizacion").as_posix()
GOLD_PATH_DIM_EMPRESA = (LAKE_DIR / "gold" / "dim_empresa").as_posix()
GOLD_PATH_DIM_SECTOR = (LAKE_DIR / "gold" / "dim_sector").as_posix()
GOLD_PATH_DIM_FECHA = (LAKE_DIR / "gold" / "dim_fecha").as_posix()