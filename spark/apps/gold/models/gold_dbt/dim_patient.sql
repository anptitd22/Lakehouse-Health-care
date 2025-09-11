{{ config(materialized='table') }}

WITH src AS (
  SELECT * FROM {{ source('silver', 'patient') }}
),
dedup AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY patient_id ORDER BY ingested_at DESC) AS rn
  FROM src
)
SELECT
  abs(xxhash64(cast(patient_id as string)))                AS patient_sk,
  patient_id                                               AS patient_nk,
  patient_name,
  patient_gender,
  to_date(patient_dob)                                     AS patient_dob,
  patient_address,
  patient_phone,
  patient_email
FROM dedup
WHERE rn = 1;