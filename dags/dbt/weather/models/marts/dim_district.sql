-- models/marts/dimensions/dim_district.sql
{{ config(materialized='table') }}
with base as (
    select distinct
        name as district,
        lat,
        lon,
        tz_id,
        country
    from {{ ref('stg_weather_data') }}
),

country_dim as (
    select
        country,
        {{ dbt_utils.surrogate_key(['country']) }} as country_id
    from {{ ref('dim_country') }}
)

select
    {{ dbt_utils.surrogate_key(['district', 'lat', 'lon', 'tz_id', 'c.country_id']) }} as district_id,
    b.district,
    b.lat,
    b.lon,
    b.tz_id,
    c.country_id
from base b
join country_dim c on b.country = c.country
