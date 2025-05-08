{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='mart_core',
    on_schema_change='sync_all_columns',
    merge_update_columns=['code', 'description', 'source_dlu', 'dlu']
) }}

WITH source_status AS (
    SELECT
        DISTINCT status,
        status_desc,
        dl_upd AS source_dlu
    FROM {{ ref('stg_lapd_crime_data') }}
    WHERE status IS NOT NULL

    {% if is_incremental() %}
    AND dl_upd > (SELECT MAX(source_dlu) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS id,
    status AS code,
    status_desc AS description,
    source_dlu,
    CURRENT_TIMESTAMP() AS dlu
FROM source_status
