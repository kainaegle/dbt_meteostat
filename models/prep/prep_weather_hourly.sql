WITH hourly_data AS (
    SELECT * 
    FROM {{ ref('staging_weather_hourly') }}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date
        , timestamp::TIME AS time
        , TO_CHAR(timestamp, 'HH24:MI') AS hour
        , TO_CHAR(timestamp, 'FMmonth') AS month_name
        , TO_CHAR(timestamp, 'FMday') AS weekday
        , DATE_PART('day', timestamp) AS date_day
        , DATE_PART('month', timestamp) AS date_month
        , DATE_PART('year', timestamp) AS date_year
        , DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , (CASE
            WHEN time BETWEEN TIME '00:00:00' AND TIME '05:59:59' THEN 'night'
            WHEN time BETWEEN TIME '06:00:00' AND TIME '17:59:59' THEN 'day'
            WHEN time BETWEEN TIME '18:00:00' AND TIME '23:59:59' THEN 'evening'
          END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features