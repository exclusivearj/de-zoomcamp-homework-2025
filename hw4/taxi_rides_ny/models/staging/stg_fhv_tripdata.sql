{{
    config(
        materialized='view'
    )
}}

  select 
    * 
  from 
    {{ source('staging','fhv_tripdata') }}
  where
    dispatching_base_num IS NOT NULL

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}