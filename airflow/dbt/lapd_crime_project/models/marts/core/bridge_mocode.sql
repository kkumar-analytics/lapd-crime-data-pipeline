{{ config(
    materialized='incremental',
    schema='mart_core',
    unique_key=['dr_no', 'mocode_dim_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        src.dr_no,
        CAST(flattened_mocode.value AS INT) AS mocode_value,
        CURRENT_TIMESTAMP() AS load_time
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
),
joined_data AS (
    SELECT
        fsd.dr_no,
        fsd.mocode_dim_id,
        fsd.load_time,
        existing.doe
    FROM final_source_data fsd
    LEFT JOIN {{ this }} AS existing
        ON fsd.dr_no = existing.dr_no
       AND fsd.mocode_dim_id = existing.mocode_dim_id
)

SELECT
    dr_no,
    mocode_dim_id,
    COALESCE(doe, load_time) AS doe,
    load_time AS dlu
FROM joined_data
