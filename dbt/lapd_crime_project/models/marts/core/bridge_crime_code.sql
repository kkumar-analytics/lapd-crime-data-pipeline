{{ config(
    materialized='incremental',
    schema = 'mart_core',
    unique_key=['dr_no', 'crime_code_dim_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        src.dr_no,
        dim.id AS crime_code_dim_id,
        CURRENT_TIMESTAMP AS load_time
    FROM
        {{ ref('stg_lapd_crime_data') }} AS src
    CROSS JOIN LATERAL FLATTEN(input => ARRAY_CONSTRUCT(src.crm_cd_1, src.crm_cd_2, src.crm_cd_3, src.crm_cd_4)) AS flattened_crime_code
    LEFT JOIN {{ ref('dim_crime_code') }} AS dim
        ON flattened_crime_code.value = dim.code
    WHERE dim.id IS NOT NULL

    {% if is_incremental() %}
      AND src.dl_upd > (SELECT MAX(dlu) FROM {{ this }})
    {% endif %}
)

SELECT
    dr_no,
    crime_code_dim_id,
    {% if is_incremental() %}
        COALESCE(
            (SELECT doe FROM {{ this }} WHERE dr_no = source_data.dr_no AND crime_code_dim_id = source_data.crime_code_dim_id),
            load_time
        ) AS doe,
    {% else %}
        load_time AS doe,
    {% endif %}
    load_time AS dlu
FROM source_data
