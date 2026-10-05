import os

UC_CATALOG = "workspace"
UC_SCHEMA = "bronze"
UC_VOLUME = "raw_landing"
VOLUME_SUBDIR = "gharchive"

GHARCHIVE_BASE_URL = "https://data.gharchive.org"
TARGET_CHUNK_BYTES = 8 * 1024**3  # ~8 GB per parallel job (sizing guide)

VOLUME_ROOT = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}/{VOLUME_SUBDIR}"


def get_databricks_host() -> str:
    return os.environ["DATABRICKS_INSTANCE"].rstrip("/")


def get_databricks_token() -> str:
    return os.environ["DATABRICKS_TOKEN"]
