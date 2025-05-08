{% snapshot dim_location %}
{{
    config(
        target_schema='mart_core',
        unique_key='id',
        strategy='check',
        check_cols=['location', 'cross_street', 'lat', 'lon']
    )
}}
with source_date as (
SELECT
    distinct
    location,
    cross_street,
    lat,
    lon,
    dl_upd AS source_dlu,
    CURRENT_TIMESTAMP AS dlu
FROM {{ ref('stg_lapd_crime_data') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['location', 'cross_street', 'lat', 'lon']) }} AS id,
    location,
    cross_street,
    lat,
    lon,
    source_dlu,
    dlu
FROM source_date

{% endsnapshot %}
