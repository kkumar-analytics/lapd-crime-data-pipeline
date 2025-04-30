{{ config(
    materialized='table',
    schema='mart_reporting',
    tags=['reporting', 'summary']
) }}

SELECT
    dv.age AS victim_age,
    dv.sex AS victim_sex,
    dv.descent AS victim_descent,
    COUNT(*) AS incident_count,
    SUM(COUNT(*)) OVER (PARTITION BY dv.age, dv.sex) AS total_by_age_sex,
    ROUND(
        (COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (PARTITION BY dv.age, dv.sex),
        2
    ) AS descent_percentage
FROM
    {{ ref('fct_crime_events') }} AS fce
    JOIN {{ ref('dim_victim') }} AS dv ON fce.victims_id = dv.id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
