#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype_tz = {
    "LocationID": "Int64",
    "Borough": "object",
    "Zone": "object",
    "service_zone": "object"
}

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='pgdatabase', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table-green-taxi', default='green_taxi_data', help='Target table name')
@click.option('--target-table-taxi-zone', default='taxi_zone_lookup', help='Taxi Zone table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table_green_taxi, target_table_taxi_zone):
    """Ingest NYC taxi data into PostgreSQL database."""
    url_green_taxi = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    url_taxi_zone = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_green_taxi = pd.read_parquet(url_green_taxi)
    df_taxi_zone = pd.read_csv(url_taxi_zone, dtype=dtype_tz)

    df_green_taxi.to_sql(
        name=target_table_green_taxi,
        con=engine,
        if_exists='append'
    )

    df_taxi_zone.to_sql(
    name=target_table_taxi_zone,
    con=engine,
    if_exists='append'
    )
    print("finished!")


if __name__ == '__main__':
    run()