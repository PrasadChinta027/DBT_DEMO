{{
    config(
        materialized='table'
    )
}}

with CTE as(
    Select 
    to_timestamp(started_at) as started_at,
    date(to_timestamp(started_at)) as Date_started_at,
    hour(to_timestamp(started_at)) as hour_started_at,
    {{ day_type('STARTED_AT') }} as DAY_TYPE,
    {{ get_season('STARTED_AT') }} as STATION_OF_YEAR
    FROM {{ ref('stg_bike') }}
    where started_at <> 'started_at'
)

Select * from CTE