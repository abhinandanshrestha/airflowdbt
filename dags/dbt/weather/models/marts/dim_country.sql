-- models/marts/dimensions/dim_country.sql
{{ config(materialized='table') }}
with base as (
    select distinct country
    from {{ ref('stg_weather_data') }}
)

select
    {{ dbt_utils.surrogate_key(['country']) }} as country_id,
    country
from base
