with CTE as(
    Select 
    to_timestamp(started_at) as started_at,
    date(to_timestamp(started_at)) as Date_started_at,
    hour(to_timestamp(started_at)) as hour_started_at,
    case 
        when dayname(to_timestamp(started_at)) in ('sat', 'sun') then 'WEEKEND'
        else 'BUSINESSDAY'
    end as DAY_TYPE,
    case
        when month(to_timestamp(started_at)) in (12,1,2) then 'WINTER'
        when month(to_timestamp(started_at)) in (3,4,5) then 'SPRING'
        when month(to_timestamp(started_at)) in (6,7,8) then 'SUMMER'
        else 'AUTUMN'
    end as STATION_OF_YEAR
    FROM {{ source('demo', 'bike') }}
    where started_at <> 'started_at'
)

Select * from CTE