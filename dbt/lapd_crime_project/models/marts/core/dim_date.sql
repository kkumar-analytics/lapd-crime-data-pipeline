{{ config(
    materialized='table',
    schema='mart_core'
) }}

WITH date_ AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS id,
    date_day AS full_date,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    UPPER(TO_CHAR(date_day, 'MON')) AS month_name,
    EXTRACT(day FROM date_day) AS day,
    EXTRACT(dow FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    EXTRACT(week FROM date_day) AS week_of_year,
    EXTRACT(quarter FROM date_day) AS quarter,
    CASE WHEN EXTRACT(dow FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CURRENT_TIMESTAMP() AS doe
FROM date_
