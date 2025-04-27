{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='mart_core',
    on_schema_change='sync_all_columns',
    merge_update_columns=['age', 'sex', 'descent', 'source_dlu', 'dlu']
) }}

WITH source_victim AS (
    SELECT
        DISTINCT vict_age,
        vict_sex,
        vict_descent,
        dl_upd AS source_dlu
    FROM {{ ref('stg_lapd_crime_data') }}

    {% if is_incremental() %}
    WHERE dl_upd > (SELECT MAX(source_dlu) FROM {{ this }})
    {% endif %}
),

descent_mapping AS (
    SELECT
        descent_code,
        descent_description
    FROM {{ ref('descent_mapping') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vict_age', 'vict_sex', 'vict_descent']) }} AS id,
    vict_age AS age,
    vict_sex AS sex,
    dm.descent_description AS descent,
    source_dlu,
    CURRENT_TIMESTAMP AS dlu
FROM source_victim sv
LEFT JOIN descent_mapping dm
    ON sv.vict_descent = dm.descent_code
