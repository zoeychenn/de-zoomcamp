"""dlt pipeline to ingest data from the Open Library REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """
    Define dlt resources for the Open Library REST API.

    Starts with the `books` endpoint as described in `open_library-docs.yaml`:
    - Base URL: https://openlibrary.org
    - Endpoint: GET /api/books
    Incremental loading is intentionally omitted for now.
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    # Response is {"ISBN:xxx": {...}, "ISBN:yyy": {...}}. Select all values.
                    "data_selector": "$.*",
                    # Example request: fetch metadata for a small set of ISBNs.
                    "params": {
                        "bibkeys": "ISBN:0451526538,ISBN:0201558025",
                        "format": "json",
                        "jscmd": "data",
                    },
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
