{{ config(
    materialized = 'table',
    schema = 'mart_core',
    on_schema_change = 'sync_all_columns'
) }}

WITH source_area AS (
    SELECT
        DISTINCT area,
        area_name,
        dl_upd AS source_dlu
    FROM {{ ref('stg_lapd_crime_data') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['area']) }} AS id,
    area AS code,
    area_name AS name,
    source_dlu,
    CURRENT_TIMESTAMP AS dlu
FROM source_area
