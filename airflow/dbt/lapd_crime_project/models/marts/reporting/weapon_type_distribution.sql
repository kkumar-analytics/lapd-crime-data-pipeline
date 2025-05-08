{{ config(
    materialized='table',
    schema='mart_reporting',
    tags=['reporting', 'summary']
) }}

SELECT
    da.name AS area_name,
    dt.part_of_day,
    dw.description AS weapon_type,
    COUNT(*) AS weapon_count,
    SUM(COUNT(*)) OVER (PARTITION BY da.name, dt.part_of_day) AS total_crimes,
    ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (PARTITION BY da.name, dt.part_of_day), 2) AS weapon_type_percentage
FROM
    {{ ref('fct_crime_events') }} AS fce
    JOIN {{ ref('dim_time') }} AS dt ON fce.time_occ_id = dt.id
    JOIN {{ ref('dim_area') }} AS da ON fce.area_dim_id = da.id
    JOIN {{ ref('dim_weapon') }} AS dw ON fce.weapons_id = dw.id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
