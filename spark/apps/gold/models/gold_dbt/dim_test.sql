{{ config(materialized='table') }}

WITH src AS (
  SELECT * FROM {{ source('silver', 'lab_results') }}
),
norm AS (
  SELECT
    trim(lower(test_type)) AS test_type,
    trim(lower(parameter)) AS parameter,
    trim(lower(unit))      AS unit,
    trim(lower(normal_range)) AS normal_range
  FROM src
)
SELECT
  abs(xxhash64(coalesce(test_type,'') || '|' || coalesce(parameter,'') || '|' || coalesce(unit,''))) AS test_sk,
  concat_ws('|', coalesce(test_type,''), coalesce(parameter,''), coalesce(unit,''))                  AS test_nk,
  test_type,
  parameter,
  unit,
  normal_range
FROM norm
GROUP BY test_type, parameter, unit, normal_range;