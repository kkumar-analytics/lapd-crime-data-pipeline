{{ config(
    materialized='table',
    schema='mart_core'
) }}

select
  {{ dbt_utils.generate_surrogate_key(['mocodes']) }} as id,
  mocodes as code,
  description,
  current_timestamp() as dlu
from {{ ref('mocodes') }}