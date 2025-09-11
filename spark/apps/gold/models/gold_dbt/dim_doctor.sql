{{ config(materialized='table') }}

WITH src AS (
  SELECT * FROM {{ source('silver', 'doctors') }}
),
dedup AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY doctor_id ORDER BY ingested_at DESC) AS rn
  FROM src
)
SELECT
  abs(xxhash64(cast(doctor_id as string)))                 AS doctor_sk,
  doctor_id                                                AS doctor_nk,
  doctor_name,
  doctor_specialization,
  doctor_phone,
  doctor_email
FROM dedup
WHERE rn = 1;