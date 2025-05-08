{{ config(
    materialized='table',
    schema='mart_reporting',
    tags=['reporting', 'summary']
) }}

WITH crime_data AS (
    SELECT
        ROUND(dl.lat, 3) AS lat_bucket,
        ROUND(dl.lon, 3) AS lon_bucket,
        COUNT(*) AS crime_count,
        dc.code AS crime_code,
        dc.description AS crime_description,
        dd.month_name AS month,
        dd.year AS year,
        dd.month AS month_number,
        dt.part_of_day AS time_of_day
    FROM {{ ref('fct_crime_events') }} AS fce
    JOIN {{ ref('dim_location') }} AS dl ON fce.location_dim_id = dl.id
    JOIN {{ ref('bridge_crime_code') }} AS bcc ON fce.dr_no = bcc.dr_no
    JOIN {{ ref('dim_crime_code') }} AS dc ON bcc.crime_code_dim_id = dc.id
    JOIN {{ ref('dim_date') }} AS dd ON fce.date_occ_id = dd.id
    JOIN {{ ref('dim_time') }} AS dt ON fce.time_occ_id = dt.id
    GROUP BY 1,2,4,5,6,7,8,9
)
SELECT
    lat_bucket,
    lon_bucket,
    crime_code,
    crime_description,
    month,
    year,
    month_number,
    time_of_day,
    crime_count,
    SUM(crime_count) OVER (PARTITION BY lat_bucket, lon_bucket) AS total_crimes_in_grid,
    ROUND((crime_count * 100.0) / SUM(crime_count) OVER (PARTITION BY lat_bucket, lon_bucket), 2) AS crime_density_percentage
FROM crime_data
ORDER BY lat_bucket, lon_bucket, crime_code, year, month_number

