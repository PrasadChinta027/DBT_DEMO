{{
    config(
        materialized='table'
    )
}}

with trips as(
    Select
    ride_id,
    -- rideable_type,
    date(to_timestamp(started_at)) as trip_date,
    start_station_id,
    end_station_id,
    member_casual,
    timestampdiff(second, to_timestamp(started_at), to_timestamp(ended_at)) as trip_duration_seconds
    from {{ ref('stg_bike') }}
)

Select * from trips