with source as (
    select * from weather_daily
)

select
    name,
    lat,
    lon,
    tz_id,
    country,
    temp_c,
    humidity,
    pressure_mb,
    wind_kph,
    condition_text,
    "time"::timestamp as time_recorded
from source