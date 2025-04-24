-- models/marts/facts/fact_weather_data.sql
{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_weather_data') }}
),

district_dim as (
    select
        district,
        lat,
        lon,
        tz_id,
        country_id,
        {{ dbt_utils.surrogate_key(['district', 'lat', 'lon', 'tz_id', 'country_id']) }} as district_id
    from {{ ref('dim_district') }}
)

select
    row_number() over(ORDER BY b.time_recorded) as id,
    d.district_id,
    b.temp_c,
    b.humidity,
    b.pressure_mb,
    b.wind_kph,
    b.condition_text,
    b.time_recorded
from base b
join district_dim d
    on b.name = d.district
    and b.lat = d.lat
    and b.lon = d.lon
    and b.tz_id = d.tz_id
