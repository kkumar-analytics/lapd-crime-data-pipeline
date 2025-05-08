{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'raw_lapd_crime_data') }}
)
select
    cast(dr_no as int) as dr_no,
    TO_DATE(date_rptd, 'MM/DD/YYYY HH:MI:SS AM') AS date_rptd,
    TO_DATE(date_occ, 'MM/DD/YYYY HH:MI:SS AM') AS date_occ,
    to_time(lpad(cast(time_occ as string), 4, '0'), 'HH24MI') as time_occ,
    cast(area as integer) as area,
    area_name,
    cast(rpt_dist_no as int) as rpt_dist_no,
    cast(crm_cd as int) as crm_cd,
    crm_cd_desc,
    mocodes,
    cast(vict_age as integer) as vict_age,
    case
        when vict_sex = 'M' then 'Male'
        when vict_sex = 'F' then 'Female'
        when vict_sex = 'X' then 'Unknown'
        else NULL
    end as vict_sex,
    vict_descent,
    cast(premis_cd as int) as premis_cd,
    premis_desc,
    cast(weapon_used_cd as int) as weapon_used_cd,
    weapon_desc,
    status,
    status_desc,
    cast(crm_cd_1 as int) as crm_cd_1,
    cast(crm_cd_2 as int) as crm_cd_2,
    cast(crm_cd_3 as int) as crm_cd_3,
    cast(crm_cd_4 as int) as crm_cd_4,
    location,
    cross_street,
    cast(lat as float) as lat,
    cast(lon as float) as lon,
    dl_upd
from source
