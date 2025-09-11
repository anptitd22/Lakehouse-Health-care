{% snapshot patient_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='patient_id',
    strategy='check',
    check_cols=['patient_name','patient_gender','patient_dob','patient_address','patient_phone','patient_email']
  )
}}
select
  patient_id,
  patient_name,
  patient_gender,
  patient_dob,
  patient_address,
  patient_phone,
  patient_email,
  coalesce(cast(ingested_at as timestamp), current_timestamp()) as src_updated_at
from {{ source('silver','patient') }}
{% endsnapshot %}
