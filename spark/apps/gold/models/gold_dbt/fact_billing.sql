{{ config(materialized='table') }}

WITH f AS (
  SELECT * FROM {{ source('silver', 'hospital_fee') }}
)
SELECT
  fee_id,
  abs(xxhash64(cast(appointment_id as string))) AS appointment_sk,
  abs(xxhash64(cast(patient_id as string)))     AS patient_sk,
  to_date(fee_date)                              AS fee_date,
  amount,
  abs(xxhash64(trim(lower(coalesce(service_type,''))) || '|' || trim(lower(coalesce(description,''))))) AS service_sk
FROM f;