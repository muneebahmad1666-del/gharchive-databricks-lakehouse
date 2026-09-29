import pytest

from gharchive_lakehouse.plan_gharchive_chunks import plan_gharchive_chunks


def test_chunk_count_and_size():
    chunks = plan_gharchive_chunks("2023-01-01", days=6, days_per_chunk=3)
    assert len(chunks) == 2
    assert all(len(c) == 72 for c in chunks)


def test_url_format_has_unpadded_hour():
    chunks = plan_gharchive_chunks("2023-01-01", days=1, days_per_chunk=1)
    assert chunks[0][0] == "https://data.gharchive.org/2023-01-01-0.json.gz"
    assert chunks[0][23] == "https://data.gharchive.org/2023-01-01-23.json.gz"


def test_last_chunk_can_be_partial():
    chunks = plan_gharchive_chunks("2023-01-01", days=4, days_per_chunk=3)
    assert [len(c) for c in chunks] == [72, 24]


def test_invalid_args():
    with pytest.raises(ValueError):
        plan_gharchive_chunks("2023-01-01", days=0)
