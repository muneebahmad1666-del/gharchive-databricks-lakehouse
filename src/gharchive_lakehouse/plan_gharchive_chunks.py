from datetime import datetime, timedelta

from gharchive_lakehouse.config import GHARCHIVE_BASE_URL


def plan_gharchive_chunks(start_date: str, days: int = 30, days_per_chunk: int = 3) -> list[list[str]]:
    """Split `days` of GH Archive hourly files into chunks of `days_per_chunk` days.

    GH Archive file names use a non-zero-padded hour: 2023-01-01-0.json.gz ... 2023-01-01-23.json.gz
    """
    if days < 1 or days_per_chunk < 1:
        raise ValueError("days and days_per_chunk must be >= 1")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    hours = [start + timedelta(days=d, hours=h) for d in range(days) for h in range(24)]
    size = days_per_chunk * 24
    return [
        [f"{GHARCHIVE_BASE_URL}/{dt:%Y-%m-%d}-{dt.hour}.json.gz" for dt in hours[i : i + size]]
        for i in range(0, len(hours), size)
    ]
