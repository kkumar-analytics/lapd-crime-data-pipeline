{{ config(
    materialized='incremental',
    unique_key='code',
    incremental_strategy='merge',
    schema='mart_core',
    on_schema_change='sync_all_columns',
    merge_update_columns=['description', 'source_dlu', 'dlu']
) }}

with stage_data as (
  select
    distinct
    CRM_CD as code,
    CRM_CD_DESC as description,
    dl_upd as source_dlu
from {{ ref('stg_lapd_crime_data') }}

{% if is_incremental() %}
  where dl_upd > (select max(source_dlu) from {{ this }})
{% endif %}
)
select
  {{ dbt_utils.generate_surrogate_key(['code']) }} as id,
  code,
  description,
  source_dlu,
  current_timestamp as dlu
from stage_data
