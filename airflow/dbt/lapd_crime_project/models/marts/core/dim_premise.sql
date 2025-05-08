{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    schema = 'mart_core',
    on_schema_change = 'sync_all_columns',
    merge_update_columns = ['description', 'source_dlu', 'dlu']
) }}

WITH source_premise AS (
    SELECT
        DISTINCT premis_cd,
        premis_desc,
        dl_upd AS source_dlu
    FROM {{ ref('stg_lapd_crime_data') }}
    WHERE premis_cd IS NOT NULL
      AND premis_desc IS NOT NULL
      {% if is_incremental() %}
      AND dl_upd > (SELECT MAX(source_dlu) FROM {{ this }})
      {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['premis_cd']) }} AS id,
    premis_cd AS code,
    premis_desc AS description,
    source_dlu,
    CURRENT_TIMESTAMP() AS dlu
FROM source_premise
