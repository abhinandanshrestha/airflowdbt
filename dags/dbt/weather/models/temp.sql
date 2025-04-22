{{ config(materialized='table') }}

with source_data as (
    SELECT *
    from weather
    limit 3
)

select *
from source_data
