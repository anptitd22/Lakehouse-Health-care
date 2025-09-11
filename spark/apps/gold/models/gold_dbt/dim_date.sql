{{ config(materialized='table') }}

-- Build a standard calendar dimension (2018..2032). Adjust ranges as needed.
WITH dates AS (
  SELECT explode(sequence(to_date('2018-01-01'), to_date('2032-12-31'), interval 1 day)) AS d
)
SELECT
  year(d) * 10000 + month(d) * 100 + day(d)       AS date_key,
  d                                               AS date,
  year(d)                                         AS year,
  quarter(d)                                      AS quarter,
  month(d)                                        AS month,
  day(d)                                          AS day_of_month,
  dayofweek(d)                                    AS day_of_week,     -- 1..7 (Sun..Sat)
  weekofyear(d)                                   AS week_of_year,
  CASE WHEN dayofweek(d) IN (1,7) THEN true ELSE false END AS is_weekend
FROM dates;