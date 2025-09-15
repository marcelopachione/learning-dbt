with source as (
    select *
    from {{ source('weather', 'city_weather') }}
),
stg_weather_report as (
    select
        id,
        city,
        temperature,
        weather_description,
        wind_speed,
        time as weather_time_local,
        {{ convert_to_local('inserted_at', 'timezone') }} as inserted_at_local
    from source
)
select *
from stg_weather_report