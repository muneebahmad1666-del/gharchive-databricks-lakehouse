import logging
import os
import tempfile

import requests

from gharchive_lakehouse.config import VOLUME_ROOT, get_databricks_host, get_databricks_token

log = logging.getLogger(__name__)


def _volume_file_url(host: str, file_name: str) -> str:
    return f"{host}/api/2.0/fs/files{VOLUME_ROOT}/{file_name}"


def upload_chunk_files(urls: list[str]) -> dict[str, int]:
    """Download each GH Archive file and upload it to the Unity Catalog Volume.

    Idempotent: files already in the Volume are skipped, so a retried task resumes.
    """
    host = get_databricks_host()
    headers = {"Authorization": f"Bearer {get_databricks_token()}"}
    uploaded = skipped = missing = 0
    failed: list[str] = []

    for url in urls:
        file_name = url.rsplit("/", 1)[-1]
        target = _volume_file_url(host, file_name)
        try:
            if requests.head(target, headers=headers, timeout=30).status_code == 200:
                skipped += 1
                continue

            with requests.get(url, stream=True, timeout=(30, 300)) as src:
                if src.status_code == 404:
                    missing += 1
                    log.warning("Not published on GH Archive: %s", url)
                    continue
                src.raise_for_status()
                with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
                    for block in src.iter_content(chunk_size=8 * 1024 * 1024):
                        tmp.write(block)
                    tmp_path = tmp.name

            try:
                with open(tmp_path, "rb") as fh:
                    put = requests.put(
                        target, headers=headers, params={"overwrite": "true"}, data=fh, timeout=900
                    )
                put.raise_for_status()
            finally:
                os.remove(tmp_path)
            uploaded += 1
        except requests.exceptions.RequestException as exc:
            failed.append(file_name)
            log.warning("Failed %s: %s", file_name, exc)

    summary = {"uploaded": uploaded, "skipped": skipped, "missing": missing, "failed": len(failed)}
    log.info("Chunk summary: %s", summary)
    if failed:  # fail the task so Airflow retries; finished files will be skipped
        raise RuntimeError(f"{len(failed)} file(s) failed, e.g. {failed[:3]}")
    return summary
