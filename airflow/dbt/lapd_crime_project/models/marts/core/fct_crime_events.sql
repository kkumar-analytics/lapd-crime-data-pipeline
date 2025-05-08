{{ config(
    materialized = 'incremental',
    schema = 'mart_core',
    unique_key = ['dr_no'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        src.dr_no,
        date_occ_dim.id     AS date_occ_id,
        date_rptd_dim.id    AS date_rptd_id,
        time_occ_dim.id     AS time_occ_id,
        victim_dim.id       AS victims_id,
        weapon_dim.id       AS weapons_id,
        status_dim.id       AS status_dim_id,
        area_dim.id         AS area_dim_id,
        premise_dim.id      AS premis_dim_id,
        location_dim.id     AS location_dim_id,
        CURRENT_TIMESTAMP   AS load_time
    FROM {{ ref('stg_lapd_crime_data') }} AS src
    LEFT JOIN {{ ref('dim_date') }} AS date_occ_dim
        ON src.date_occ = date_occ_dim.full_date
    LEFT JOIN {{ ref('dim_date') }} AS date_rptd_dim
        ON src.date_rptd = date_rptd_dim.full_date
    LEFT JOIN {{ ref('dim_time') }} AS time_occ_dim
        ON EXTRACT(HOUR FROM src.time_occ) = time_occ_dim.hour_of_day
        AND EXTRACT(MINUTE FROM src.time_occ) = time_occ_dim.minute_of_hour
    LEFT JOIN (
        SELECT
            vd.id,
            vd.age,
            vd.sex,
            dm.descent_code
        FROM {{ ref('dim_victim') }} AS vd
        LEFT JOIN {{ ref('descent_mapping') }} AS dm
            ON vd.descent = dm.descent_description
    ) AS victim_dim
        ON IFNULL(src.vict_age, -1) = IFNULL(victim_dim.age, -1)
        AND IFNULL(src.vict_sex, 'NA') = IFNULL(victim_dim.sex, 'NA')
        AND IFNULL(src.vict_descent, 'NA') = IFNULL(victim_dim.descent_code, 'NA')
    LEFT JOIN {{ ref('dim_weapon') }} AS weapon_dim
        ON src.weapon_used_cd = weapon_dim.code
    LEFT JOIN {{ ref('dim_status') }} AS status_dim
        ON src.status = status_dim.code
    LEFT JOIN {{ ref('dim_area') }} AS area_dim
        ON src.area = area_dim.code
    LEFT JOIN {{ ref('dim_premise') }} AS premise_dim
        ON src.premis_cd = premise_dim.code
    LEFT JOIN {{ ref('dim_location') }} AS location_dim
        ON IFNULL(src.location, 'NA') = IFNULL(location_dim.location, 'NA')
        AND IFNULL(src.cross_street, 'NA') = IFNULL(location_dim.cross_street, 'NA')
        AND IFNULL(src.lat, 0.0) = IFNULL(location_dim.lat, 0.0)
        AND IFNULL(src.lon, 0.0) = IFNULL(location_dim.lon, 0.0)

    {% if is_incremental() %}
        WHERE src.dl_upd > (SELECT MAX(dlu) FROM {{ this }})
    {% endif %}
)

SELECT
    source.dr_no,
    source.date_rptd_id,
    source.date_occ_id,
    source.time_occ_id,
    source.victims_id,
    source.weapons_id,
    source.status_dim_id,
    source.area_dim_id,
    source.premis_dim_id,
    source.location_dim_id,
    {% if is_incremental() %}
        existing.doe,
    {% else %}
        load_time AS doe,
    {% endif %}
    load_time AS dlu
FROM source_data AS source
LEFT JOIN {{ this }} AS existing
    ON source.dr_no = existing.dr_no
