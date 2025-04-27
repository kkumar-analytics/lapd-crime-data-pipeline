{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='mart_core',
    on_schema_change='sync_all_columns',
    merge_update_columns=['code', 'description', 'source_dlu', 'dlu']
) }}

WITH source_weapon AS (
    SELECT
        DISTINCT weapon_used_cd,
        weapon_desc,
        dl_upd AS source_dlu
    FROM {{ ref('stg_lapd_crime_data') }}
    WHERE weapon_used_cd IS NOT NULL

    {% if is_incremental() %}
    AND dl_upd > (SELECT MAX(source_dlu) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['weapon_used_cd']) }} AS id,
    weapon_used_cd AS code,
    weapon_desc AS description,
    source_dlu,
    CURRENT_TIMESTAMP() AS dlu
FROM source_weapon
