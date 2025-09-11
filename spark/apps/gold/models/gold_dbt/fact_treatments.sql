{{ config(materialized='table') }}

WITH t AS (
  SELECT * FROM {{ source('silver', 'treatments') }}
)
SELECT
  treatment_id,
  abs(xxhash64(cast(patient_id as string))) AS patient_sk,
  abs(xxhash64(cast(doctor_id as string)))  AS doctor_sk,
  abs(xxhash64(cast(disease_id as string))) AS disease_sk,
  to_date(treatment_date)                   AS treatment_date,
  1                                         AS treatment_cnt
FROM t;