{{ config(materialized='table') }}

WITH l AS (
  SELECT * FROM {{ source('silver', 'lab_results') }}
),
norm AS (
  SELECT
    lab_result_id,
    abs(xxhash64(cast(appointment_id as string))) AS appointment_sk,
    abs(xxhash64(cast(patient_id as string)))     AS patient_sk,
    to_date(test_date)                            AS test_date,
    trim(lower(test_type)) AS test_type,
    trim(lower(parameter)) AS parameter,
    trim(lower(unit))      AS unit,
    -- strip non-numeric to cast (handles strings like "5.4 mmol/L")
    cast(regexp_replace(value, '[^0-9\\.\\-]', '') AS double) AS numeric_value,
    interpretation
  FROM l
)
SELECT
  lab_result_id,
  appointment_sk,
  patient_sk,
  test_date,
  abs(xxhash64(coalesce(test_type,'') || '|' || coalesce(parameter,'') || '|' || coalesce(unit,''))) AS test_sk,
  numeric_value,
  interpretation
FROM norm;