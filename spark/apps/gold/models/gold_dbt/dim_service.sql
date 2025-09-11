{{ config(materialized='table') }}

WITH src AS (
  SELECT * FROM {{ source('silver', 'hospital_fee') }}
),
norm AS (
  SELECT
    trim(lower(service_type)) AS service_type,
    trim(lower(description))  AS description
  FROM src
)
SELECT
  abs(xxhash64(coalesce(service_type,'') || '|' || coalesce(description,''))) AS service_sk,
  concat_ws('|', coalesce(service_type,''), coalesce(description,''))         AS service_nk,
  service_type,
  description
FROM norm
GROUP BY service_type, description;