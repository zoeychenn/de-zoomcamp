/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: delete+insert
  incremental_key: pickup_datetime

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    primary_key: true
    checks:
      - name: not_null
  - name: PULocationID
    type: float
    description: "Pick-up location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: DOLocationID
    type: float
    description: "Drop-off location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  
  # Other columns
  - name: taxi_type
    type: string
    description: "Taxi type: yellow or green"
    checks:
      - name: not_null
  - name: passenger_count
    type: float
    description: "Number of passengers"
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: payment_type
    type: float
    description: "Payment type ID"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type name from lookup"
  - name: total_amount
    type: float
    description: "Total amount in USD"
    checks:
      - name: non_negative
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the row was extracted"

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trips based on composite primary key"
    query: |
      SELECT COUNT(*) as duplicate_count
      FROM (
        SELECT 
          pickup_datetime,
          dropoff_datetime,
          PULocationID,
          DOLocationID,
          fare_amount,
          COUNT(*) as cnt
        FROM staging.trips
        GROUP BY 
          pickup_datetime,
          dropoff_datetime,
          PULocationID,
          DOLocationID,
          fare_amount
        HAVING COUNT(*) > 1
      )
    value: 0

@bruin */

WITH deduplicated_trips AS (
  SELECT 
    t.taxi_type,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.PULocationID,
    t.DOLocationID,
    t.payment_type,
    t.fare_amount,
    t.total_amount,
    t.extracted_at,
    p.payment_type_name,
    ROW_NUMBER() OVER (
      PARTITION BY 
        t.pickup_datetime,
        t.dropoff_datetime,
        t.PULocationID,
        t.DOLocationID,
        t.fare_amount
      ORDER BY t.extracted_at DESC  -- Keep most recent if duplicates exist
    ) AS rn
  FROM ingestion.trips t
  LEFT JOIN ingestion.payment_lookup p 
    ON t.payment_type = p.payment_type_id
  WHERE t.pickup_datetime >= '{{ start_datetime }}'
    AND t.pickup_datetime < '{{ end_datetime }}'
    AND t.pickup_datetime IS NOT NULL
    AND t.dropoff_datetime IS NOT NULL
    AND t.PULocationID IS NOT NULL
    AND t.DOLocationID IS NOT NULL
    AND t.fare_amount IS NOT NULL
    AND t.fare_amount >= 0
    AND t.trip_distance >= 0
    AND t.passenger_count >= 0
)
SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  fare_amount,
  taxi_type,
  passenger_count,
  trip_distance,
  payment_type,
  payment_type_name,
  total_amount,
  extracted_at
FROM deduplicated_trips
WHERE rn = 1  
