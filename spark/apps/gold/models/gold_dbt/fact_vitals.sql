{{ config(materialized='table') }}

WITH v AS (
  SELECT * FROM {{ source('silver', 'vital_signs') }}
)
SELECT
  vital_id,
  abs(xxhash64(cast(patient_id as string))) AS patient_sk,
  to_date(measurement_date)                 AS measurement_date,
  blood_pressure,
  heart_rate,
  respiratory_rate,
  temperature,
  oxygen_saturation,
  blood_sugar
FROM v;