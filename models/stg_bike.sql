{{
    config(
        materialized='table'
    )
}}

with bike as(
    Select
    RIDE_ID,
    STARTED_AT,
    ENDED_AT,
    START_STATION_NAME,
    START_STATION_ID,
    END_STATION_NAME,
    END_STATION_ID,
    START_LAT,
    START_LNG,
    END_LAT,
    END_LAN,
    MEMBER_CASUAL
    from {{ source('demo', 'bike') }}
    where ride_id <> 'bikeid'
)

select * from bike