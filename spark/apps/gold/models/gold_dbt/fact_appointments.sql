{{ config(materialized='table') }}

WITH a AS (
  SELECT * FROM {{ source('silver', 'appointment') }}
),
prep AS (
  SELECT
    abs(xxhash64(cast(appointment_id as string))) AS appointment_sk,
    abs(xxhash64(cast(patient_id as string)))     AS patient_sk,
    abs(xxhash64(cast(doctor_id as string)))      AS doctor_sk,
    to_date(appointment_date)                     AS appointment_date,
    start_time,
    end_time,
    status,
    appointment_type,
    reason,
    location,
    phone
  FROM a
),
dur AS (
  SELECT
    *,
    -- Attempt to compute duration in minutes when possible
    CASE
      WHEN start_time IS NOT NULL AND end_time IS NOT NULL THEN
        cast((unix_timestamp(to_timestamp(concat(date_format(appointment_date,'yyyy-MM-dd'),' ', end_time), 'yyyy-MM-dd HH:mm'))
             -unix_timestamp(to_timestamp(concat(date_format(appointment_date,'yyyy-MM-dd'),' ', start_time), 'yyyy-MM-dd HH:mm'))) / 60 AS INT)
      ELSE NULL
    END AS duration_min
  FROM prep
)
SELECT
  appointment_sk,
  patient_sk,
  doctor_sk,
  appointment_date,
  duration_min,
  status,
  appointment_type,
  reason,
  abs(xxhash64(coalesce(location,'') || '|' || coalesce(phone,''))) AS location_sk
FROM dur;