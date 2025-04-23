{{ config(
    materialized='table',
    schema='mart_core'
) }}

with date_ as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)
select
    row_number() over (order by date_day) as id,
    date_day as full_date,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    upper(to_char(date_day, 'MON')) as month_name,  -- Using 'MON' for 3-letter month abbreviation
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    extract(week from date_day) as week_of_year,
    extract(quarter from date_day) as quarter,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
    current_timestamp() as doe
from date_