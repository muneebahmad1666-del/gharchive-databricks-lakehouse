from datetime import datetime, timedelta
import os
import logging
import requests
from dotenv import load_dotenv
from airflow.decorators import dag, task

load_dotenv()

DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "apache")
GITHUB_REPO = os.getenv("GITHUB_REPO", "airflow")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")

UC_CATALOG, UC_SCHEMA, UC_VOLUME = "workspace", "bronze", "landing"  # matches gharchive_ingestion_dag.py now
TARGET_CHUNK_BYTES = 8 * 1024**3  # ~8 GB per job

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "muneeb_ahmad",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@task
def plan_file_chunks():
    """Fetch the full repo tree (with real sizes) and bin-pack it into ~8GB chunks."""
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/git/trees/{GITHUB_BRANCH}?recursive=1"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    files = [
        {"path": i["path"], "size": i.get("size", 0)}
        for i in res.json().get("tree", []) if i["type"] == "blob"
    ]

    chunks, current, current_size = [], [], 0
    for f in sorted(files, key=lambda x: -x["size"]):
        if current_size + f["size"] > TARGET_CHUNK_BYTES and current:
            chunks.append(current)
            current, current_size = [], 0
        current.append(f)
        current_size += f["size"]
    if current:
        chunks.append(current)
    return chunks


@task
def upload_chunk(chunk: list[dict]):
    """One of the N parallel jobs -- uploads its assigned files to the Volume."""
    github_headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    databricks_headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    uploaded, failed = 0, 0

    for f in chunk:
        try:
            raw_url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{f['path']}"
            file_res = requests.get(raw_url, headers=github_headers, timeout=60)
            file_res.raise_for_status()

            file_name = f["path"].replace("/", "_")
            volume_path = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}/files/{file_name}"
            put_res = requests.put(
                f"{DATABRICKS_INSTANCE}/api/2.0/fs/files{volume_path}",
                headers=databricks_headers, data=file_res.content, timeout=180,
            )
            put_res.raise_for_status()
            uploaded += 1
        except requests.exceptions.RequestException as e:
            failed += 1
            log.warning(f"Skipped {f['path']}: {e}")

    log.info(f"Chunk done: {uploaded} uploaded, {failed} failed")
    if uploaded == 0:
        raise ValueError("No files uploaded -- check GITHUB_OWNER/GITHUB_REPO and credentials.")


@dag(
    dag_id="github_files_ingestion_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
)
def github_files_ingestion():
    chunks = plan_file_chunks()
    upload_chunk.expand(chunk=chunks)


github_files_ingestion()