"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Taxi type: yellow or green"
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started (unified from tpep/lpep)"
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended (unified from tpep/lpep)"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: PULocationID
    type: float
    description: "Pick-up location ID"
  - name: DOLocationID
    type: float
    description: "Drop-off location ID"
  - name: payment_type
    type: float
    description: "Payment type ID (see payment_lookup)"
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
  - name: total_amount
    type: float
    description: "Total amount in USD"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the row was extracted (for lineage/debugging)"

@bruin"""

import json
import os
from io import BytesIO
from datetime import datetime

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


def materialize():
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    start_str = os.environ.get("BRUIN_START_DATE", "")
    end_str = os.environ.get("BRUIN_END_DATE", "")
    vars_str = os.environ.get("BRUIN_VARS", "{}")

    if not start_str or not end_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
    variables = json.loads(vars_str) if vars_str else {}
    taxi_types = variables.get("taxi_types", ["yellow"])

    extracted_at = pd.Timestamp.now(tz="UTC")
    frames = []

    for taxi_type in taxi_types:
        current = start_date
        while current <= end_date:
            year_month = current.strftime("%Y-%m")
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = f"{base_url}{filename}"

            try:
                resp = requests.get(url, timeout=120)
                if resp.status_code == 404:
                    # Skip missing months (e.g. not yet published or after Nov 2025)
                    current = (datetime(current.year, current.month, 1) + relativedelta(months=1)).date()
                    continue
                resp.raise_for_status()
                df = pd.read_parquet(BytesIO(resp.content))
            except requests.RequestException as e:
                raise RuntimeError(f"Failed to fetch {url}: {e}") from e

            df["taxi_type"] = taxi_type
            # Unify pickup/dropoff for yellow (tpep_*) vs green (lpep_*)
            if taxi_type == "yellow":
                df["pickup_datetime"] = df.get("tpep_pickup_datetime", pd.NaT)
                df["dropoff_datetime"] = df.get("tpep_dropoff_datetime", pd.NaT)
            else:
                df["pickup_datetime"] = df.get("lpep_pickup_datetime", pd.NaT)
                df["dropoff_datetime"] = df.get("lpep_dropoff_datetime", pd.NaT)
            df["extracted_at"] = extracted_at
            frames.append(df)

            current = (datetime(current.year, current.month, 1) + relativedelta(months=1)).date()

    if not frames:
        return pd.DataFrame(
            columns=[
                "taxi_type", "pickup_datetime", "dropoff_datetime",
                "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
                "payment_type", "fare_amount", "total_amount", "extracted_at"
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    # Ensure we return at least the columns we declared (subset for schema consistency)
    out_columns = [
        "taxi_type", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "total_amount", "extracted_at"
    ]
    for col in out_columns:
        if col not in combined.columns:
            combined[col] = None
    return combined[out_columns]
