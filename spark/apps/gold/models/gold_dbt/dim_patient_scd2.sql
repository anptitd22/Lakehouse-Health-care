{{ config(materialized='table') }}

with s as ( select * from {{ ref('patient_snapshot') }} )
select
  abs(xxhash64(cast(patient_id as string) || '|' || cast(dbt_valid_from as string))) as patient_sk,
  patient_id as patient_nk,
  patient_name,
  patient_gender,
  to_date(patient_dob) as patient_dob,
  patient_address,
  patient_phone,
  patient_email,
  cast(dbt_valid_from as timestamp) as valid_from,
  cast(dbt_valid_to   as timestamp) as valid_to,
  case when dbt_valid_to is null then true else false end as is_current
from s;
