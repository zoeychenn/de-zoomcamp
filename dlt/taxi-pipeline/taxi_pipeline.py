"""dlt pipeline to ingest NYC taxi data from the Data Engineering Zoomcamp REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_rest_api_source():
    """
    Define dlt resources for the NYC taxi Zoomcamp API.

    - Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
    - Paginated JSON, 1,000 records per page.
    - Pagination: offset-based; stop when an empty page is returned.
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api/",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "",
                    "params": {
                        "limit": 1000,
                        "offset": 0,
                    },
                    "data_selector": "$",
                    "paginator": {
                        "type": "offset",
                        "limit": 1000,
                        "offset": 0,
                        "limit_param": "limit",
                        "offset_param": "offset",
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_rest_api_source())
    print(load_info)  # noqa: T201
