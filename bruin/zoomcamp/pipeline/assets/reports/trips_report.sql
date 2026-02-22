/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: delete+insert
  incremental_key: report_date

columns:
  - name: report_date
    type: date
    description: "Date of trips (from pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type: yellow or green"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type
    type: float
    description: "Payment type ID"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type name from lookup"
  - name: trip_count
    type: bigint
    description: "Number of trips"
    checks:
      - name: not_null
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Sum of fare_amount in USD"
    checks:
      - name: not_null
      - name: non_negative
  - name: total_amount_sum
    type: float
    description: "Sum of total_amount in USD"
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_passenger_count
    type: float
    description: "Average passengers per trip"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average trip distance in miles"
    checks:
      - name: non_negative

custom_checks:
  - name: trip_count_positive_when_revenue
    description: "Rows with positive revenue should have positive trip_count"
    query: |
      SELECT COUNT(*) AS invalid_rows
      FROM reports.trips_report
      WHERE (total_fare_amount > 0 OR total_amount_sum > 0) AND trip_count <= 0
    value: 0

@bruin */

SELECT
  CAST(pickup_datetime AS DATE) AS report_date,
  taxi_type,
  payment_type,
  MAX(payment_type_name) AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare_amount,
  SUM(total_amount) AS total_amount_sum,
  AVG(passenger_count) AS avg_passenger_count,
  AVG(trip_distance) AS avg_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type
