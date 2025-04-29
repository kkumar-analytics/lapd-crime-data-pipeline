{{ config(
    materialized='incremental',
    schema = 'mart_core',
    unique_key=['dr_no', 'mocode_dim_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        src.dr_no,
        cast(flattened_mocode.value as int) AS mocode_value,
        CURRENT_TIMESTAMP AS load_time
    FROM
        {{ ref('stg_lapd_crime_data') }} AS src
    LEFT JOIN LATERAL FLATTEN(input => SPLIT(src.mocodes, ' ')) AS flattened_mocode
    WHERE flattened_mocode.value IS NOT NULL
    {% if is_incremental() %}
      AND src.dl_upd > (SELECT MAX(dlu) FROM {{ this }})
    {% endif %}
),
final_source_data AS (
    SELECT
        sd.dr_no,
        dim.id AS mocode_dim_id,
        sd.load_time
    FROM source_data sd
    JOIN {{ ref('dim_mocode') }} AS dim
        ON sd.mocode_value = dim.code
)
SELECT
    dr_no,
    mocode_dim_id,
    {% if is_incremental() %}
        COALESCE(
            (SELECT doe FROM {{ this }} WHERE dr_no = final_source_data.dr_no AND mocode_dim_id = final_source_data.mocode_dim_id),
            load_time
        ) AS doe,
    {% else %}
        load_time AS doe,
    {% endif %}
    load_time AS dlu
FROM final_source_data
