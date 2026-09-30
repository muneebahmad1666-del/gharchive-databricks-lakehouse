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

UC_CATALOG, UC_SCHEMA, UC_VOLUME = "workspace", "default", "raw_landing"
TARGET_CHUNK_BYTES = 8 * 1024**3  # ~8 GB per job

log = logging.getLogger(__name__)

@task
def plan_file_chunks():
    """Fetch the full repo tree (with real sizes) and bin-pack it into ~8GB chunks."""
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    url = "https://api.github.com/repos/OWNER/REPO/git/trees/main?recursive=1"
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
    return chunks  # e.g. 10 chunks, each ~8GB -> dynamic mapping fans these out


@task
def upload_chunk(chunk):
    """One of the N parallel ~8GB jobs -- uploads its assigned files to the Volume."""
    github_headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    databricks_headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    uploaded, failed = 0, 0

    for f in chunk:
        try:
            raw_url = f"https://raw.githubusercontent.com/OWNER/REPO/main/{f['path']}"
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


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def multi_source_ingestion():
    chunks = plan_file_chunks()
    upload_chunk.expand(chunk=chunks)  # this is the dynamic mapping -- one task instance per chunk

multi_source_ingestion()