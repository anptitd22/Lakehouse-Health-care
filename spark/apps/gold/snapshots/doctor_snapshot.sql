{% snapshot doctor_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='doctor_id',
    strategy='check',
    check_cols=['doctor_name','doctor_specialization','doctor_phone','doctor_email']
  )
}}
select
  doctor_id,
  doctor_name,
  doctor_specialization,
  doctor_phone,
  doctor_email,
  coalesce(cast(ingested_at as timestamp), current_timestamp()) as src_updated_at
from {{ source('silver','doctors') }}
{% endsnapshot %}
