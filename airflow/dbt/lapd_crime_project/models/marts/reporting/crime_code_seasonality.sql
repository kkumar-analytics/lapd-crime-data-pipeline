{{ config(
    materialized='table',
    schema='mart_reporting',
    tags=['reporting', 'summary']
) }}

SELECT
    dc.code AS crime_code,
    dc.description AS crime_description,
    dd.month_name AS month,
    dd.month AS month_number,
    COUNT(*) AS crime_count,
    SUM(COUNT(*)) OVER (PARTITION BY dc.code) AS total_by_crime,
    ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (PARTITION BY dc.code), 2) AS month_percentage
FROM
    {{ ref('bridge_crime_code') }} AS bcc
    JOIN {{ ref('dim_crime_code') }} AS dc ON bcc.crime_code_dim_id = dc.id
    JOIN {{ ref('fct_crime_events') }} AS fce ON bcc.dr_no = fce.dr_no
    JOIN {{ ref('dim_date') }} AS dd ON fce.date_occ_id = dd.id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4
