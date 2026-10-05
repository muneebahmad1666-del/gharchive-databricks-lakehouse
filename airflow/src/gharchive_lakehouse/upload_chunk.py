import logging
import os
import tempfile

import requests

from gharchive_lakehouse.config import (
    VOLUME_ROOT,
    get_databricks_host,
    get_databricks_token,
)

log = logging.getLogger(__name__)


def _volume_file_url(host: str, file_name: str) -> str:
    return f"{host}/api/2.0/fs/files{VOLUME_ROOT}/{file_name}"


def _ensure_volume_dir(host: str, headers: dict) -> None:
    """Create the target folder in the Volume (harmless if it already exists)."""
    resp = requests.put(
        f"{host}/api/2.0/fs/directories{VOLUME_ROOT}", headers=headers, timeout=30
    )
    resp.raise_for_status()


def upload_chunk_files(urls: list[str]) -> dict[str, int]:
    """Download each GH Archive file and upload it to the Unity Catalog Volume.

    Idempotent: files already in the Volume are skipped, so a retried task resumes.
    """
    host = get_databricks_host()
    headers = {"Authorization": f"Bearer {get_databricks_token()}"}
    uploaded = skipped = missing = 0
    failed: list[str] = []

    # Fail fast on bad host/token/volume instead of failing once per file.
    _ensure_volume_dir(host, headers)

    for url in urls:
        file_name = url.rsplit("/", 1)[-1]
        target = _volume_file_url(host, file_name)
        tmp_path = None
        try:
            head = requests.head(target, headers=headers, timeout=30)
            if head.status_code == 200:
                skipped += 1
                continue
            if head.status_code != 404:  # 401/403/5xx etc. are real errors
                head.raise_for_status()

            with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
                tmp_path = tmp.name  # set before downloading so cleanup always works

            with requests.get(url, stream=True, timeout=(30, 300)) as src:
                if src.status_code == 404:
                    missing += 1
                    log.warning("Not published on GH Archive: %s", url)
                    continue
                src.raise_for_status()
                with open(tmp_path, "wb") as out:
                    out.writelines(src.iter_content(chunk_size=8 * 1024 * 1024))

            with open(tmp_path, "rb") as fh:
                put = requests.put(
                    target,
                    headers=headers,
                    params={"overwrite": "true"},
                    data=fh,
                    timeout=900,
                )
            put.raise_for_status()
            uploaded += 1
        except requests.exceptions.RequestException as exc:
            failed.append(file_name)
            log.warning("Failed %s: %s", file_name, exc)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    summary = {
        "uploaded": uploaded,
        "skipped": skipped,
        "missing": missing,
        "failed": len(failed),
    }
    log.info("Chunk summary: %s", summary)
    if failed:  # fail the task so Airflow retries; finished files will be skipped
        raise RuntimeError(f"{len(failed)} file(s) failed, e.g. {failed[:3]}")
    return summary
