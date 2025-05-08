{{ config(
    materialized='incremental',
    schema='mart_core',
    unique_key=['dr_no', 'crime_code_dim_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        src.dr_no,
        dim.id AS crime_code_dim_id,
        CURRENT_TIMESTAMP() AS load_time
    FROM
        {{ ref('stg_lapd_crime_data') }} AS src
    CROSS JOIN LATERAL FLATTEN(input => ARRAY_CONSTRUCT(src.crm_cd_1, src.crm_cd_2, src.crm_cd_3, src.crm_cd_4)) AS flattened_crime_code
    LEFT JOIN {{ ref('dim_crime_code') }} AS dim
        ON flattened_crime_code.value = dim.code
    WHERE dim.id IS NOT NULL

    {% if is_incremental() %}
      AND src.dl_upd > (SELECT MAX(dlu) FROM {{ this }})
    {% endif %}
),
joined_data AS (
    SELECT
        sd.dr_no,
        sd.crime_code_dim_id,
        sd.load_time,
        existing.doe
    FROM source_data sd
    LEFT JOIN {{ this }} AS existing
      ON sd.dr_no = existing.dr_no
     AND sd.crime_code_dim_id = existing.crime_code_dim_id
)

SELECT
    dr_no,
    crime_code_dim_id,
    COALESCE(doe, load_time) AS doe,
    load_time AS dlu
FROM joined_data
