"""dlt pipeline to ingest NYC taxi data from the Data Engineering Zoomcamp REST API."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import dlt
from dlt.sources.helpers import requests

# Tune these for efficiency (fewer requests, more parallelism)
PAGE_SIZE = 1000  # Records per page; increase to 5000+ if API allows
PARALLEL_WORKERS = 20  # Number of concurrent page fetches
BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


def _fetch_page(offset: int, limit: int) -> list:
    """Fetch a single page. Returns list of records; empty list when no more data."""
    resp = requests.get(
        f"{BASE_URL}/",
        params={"limit": limit, "offset": offset},
        timeout=(10, 60),
    )
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return []
    return data if isinstance(data, list) else [data]


@dlt.source
def taxi_rest_api_source():
    """
    Define dlt resources for the NYC taxi Zoomcamp API.

    Uses parallel page fetching to speed up extraction:
    - Fetches multiple pages concurrently (PARALLEL_WORKERS).
    - Larger page size (PAGE_SIZE) reduces total HTTP requests.
    - Stops when an empty page is returned.
    """

    @dlt.resource(name="trips")
    def trips():
        offset = 0
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            while True:
                # Schedule a batch of parallel fetches
                futures = {
                    executor.submit(_fetch_page, off, PAGE_SIZE): off
                    for off in range(
                        offset,
                        offset + PARALLEL_WORKERS * PAGE_SIZE,
                        PAGE_SIZE,
                    )
                }
                batch_empty = False
                for future in as_completed(futures):
                    records = future.result()
                    if not records:
                        batch_empty = True
                        continue
                    yield records
                if batch_empty:
                    break
                offset += PARALLEL_WORKERS * PAGE_SIZE

    yield trips


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_rest_api_source())
    print(load_info)  # noqa: T201
