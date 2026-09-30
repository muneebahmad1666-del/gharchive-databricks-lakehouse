from datetime import datetime, timedelta
import logging

from gharchive_lakehouse.config import GHARCHIVE_BASE_URL

log = logging.getLogger(__name__)


def plan_gharchive_chunks(
    start_date: str, days: int = 30, hours_per_chunk: int = 6
) -> list[list[str]]:
    """Split `days` of GH Archive hourly files into small, manageable hourly chunks.

    Allows splitting by small hour increments (e.g., 6 hours) to prevent free-tier cluster crashes.
    GH Archive file names use a non-zero-padded hour: 2023-01-01-0.json.gz ... 2023-01-01-23.json.gz
    """
    if days < 1 or hours_per_chunk < 1:
        raise ValueError("days and hours_per_chunk must be >= 1")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    # Generate every explicit hour across the requested day range
    hours = [start + timedelta(days=d, hours=h) for d in range(days) for h in range(24)]
    
    # Slice the entire hours array into chunks based on the hours_per_chunk size limit
    chunks = [
        [f"{GHARCHIVE_BASE_URL}/{dt:%Y-%m-%d}-{dt.hour}.json.gz" for dt in hours[i : i + hours_per_chunk]]
        for i in range(0, len(hours), hours_per_chunk)
    ]
    
    log.info(f"Planned {len(chunks)} chunks with a max size of {hours_per_chunk} hours per chunk.")
    return chunks
