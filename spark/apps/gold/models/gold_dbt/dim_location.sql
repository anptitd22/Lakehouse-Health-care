{{ config(materialized='table') }}

WITH src AS (
  SELECT location, phone
  FROM {{ source('silver', 'appointment') }}
  WHERE location IS NOT NULL OR phone IS NOT NULL
),
norm AS (
  SELECT trim(lower(location)) AS location, trim(lower(phone)) AS phone FROM src
)
SELECT
  abs(xxhash64(coalesce(location,'') || '|' || coalesce(phone,''))) AS location_sk,
  concat_ws('|', coalesce(location,''), coalesce(phone,''))         AS location_nk,
  location,
  phone
FROM norm
GROUP BY location, phone;