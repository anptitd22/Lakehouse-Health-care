{{ config(materialized='table') }}

WITH src AS (
  SELECT * FROM {{ source('silver', 'diseases') }}
)
SELECT
  abs(xxhash64(cast(disease_id as string)))                AS disease_sk,
  disease_id                                               AS disease_nk,
  first(disease_name, true)                                AS disease_name
FROM src
GROUP BY disease_id;