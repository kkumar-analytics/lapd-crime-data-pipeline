{{ config(
    materialized='table',
    schema='mart_reporting',
    tags=['reporting', 'summary']
) }}

SELECT
    area_dim.code AS area_code,
    area_dim.name AS area_name,
    date_occ_dim.year AS year,
    date_occ_dim.month AS month,
    COUNT(*) AS crime_count,
    CURRENT_TIMESTAMP AS dlu
FROM {{ ref('fct_crime_events') }} AS fact
LEFT JOIN {{ ref('dim_date') }} AS date_occ_dim
    ON fact.date_occ_id = date_occ_dim.id
LEFT JOIN {{ ref('dim_area') }} AS area_dim
    ON fact.area_dim_id = area_dim.id
GROUP BY
    area_dim.code,
    area_dim.name,
    date_occ_dim.year,
    date_occ_dim.month
ORDER BY
    area_dim.code,
    area_dim.name,
    date_occ_dim.year,
    date_occ_dim.month
