{{ config(materialized='table') }}

with s as ( select * from {{ ref('doctor_snapshot') }} )
select
  abs(xxhash64(cast(doctor_id as string) || '|' || cast(dbt_valid_from as string))) as doctor_sk,
  doctor_id as doctor_nk,
  doctor_name,
  doctor_specialization,
  doctor_phone,
  doctor_email,
  cast(dbt_valid_from as timestamp) as valid_from,
  cast(dbt_valid_to   as timestamp) as valid_to,
  case when dbt_valid_to is null then true else false end as is_current
from s;
