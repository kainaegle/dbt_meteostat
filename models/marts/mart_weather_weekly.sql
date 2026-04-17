WITH weather_daily AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
),
weather_weekly AS (
    SELECT
        airport_code,
        DATE_TRUNC('week', date)::DATE AS week_start,
        AVG(avg_temp_c) AS avg_temp_c_weekly,
        MIN(min_temp_c) AS min_temp_c_weekly,
        MAX(max_temp_c) AS max_temp_c_weekly,
        SUM(precipitation_mm) AS precipitation_mm_weekly,
        SUM(max_snow_mm) AS snow_mm_weekly,
        AVG(avg_wind_direction) AS avg_wind_direction_weekly,
        AVG(avg_wind_speed) AS avg_wind_speed_weekly,
        MAX(avg_peakgust) AS peakgust_weekly,
        SUM(sun_minutes) AS sun_minutes_weekly
    FROM {{ ref('prep_weather_daily') }}
    GROUP BY airport_code, DATE_TRUNC('week', date)::DATE
)
SELECT *
FROM weather_weekly
ORDER BY airport_code, week_start