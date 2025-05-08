{{ config(
    materialized='table',
    schema='mart_core'
) }}

with hours_cte as (
    -- Generate hours from 0 to 23
    select row_number() over (order by seq4()) - 1 as hour
    from table(generator(rowcount => 24))
),
minutes_cte as (
    -- Generate minutes from 0 to 59 for each hour
    select row_number() over (order by seq4()) - 1 as minute
    from table(generator(rowcount => 60))
)
    SELECT
    {{ dbt_utils.generate_surrogate_key(['h.hour', 'm.minute']) }} AS id,
    h.hour AS hour_of_day,
    m.minute AS minute_of_hour,
    CASE
        WHEN h.hour BETWEEN 0 AND 5 THEN 'Night'
        WHEN h.hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN h.hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN h.hour BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Unknown'
    END AS part_of_day,
    CURRENT_TIMESTAMP AS doe
FROM hours_cte h
CROSS JOIN minutes_cte m
