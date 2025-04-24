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
),
time_dim as (
    select
        -- Generate a unique ID for each minute in a day
        row_number() over (order by h.hour, m.minute) as id,
        h.hour,
        m.minute,
        case
            when h.hour >= 0 and h.hour < 6 then 'Night'
            when h.hour >= 6 and h.hour < 12 then 'Morning'
            when h.hour >= 12 and h.hour < 18 then 'Afternoon'
            when h.hour >= 18 and h.hour < 24 then 'Evening'
            else 'Unknown'
        end as part_of_day,
        current_timestamp() as doe
    from hours_cte h
    cross join minutes_cte m
)

select
    id,
    hour as hour_of_day,
    minute as minute_of_hour,
    part_of_day,
    doe
from time_dim
