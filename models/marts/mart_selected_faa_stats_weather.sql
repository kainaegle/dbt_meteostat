WITH weather_daily AS (
    SELECT
        airport_code AS faa,
        date,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction,
        avg_wind_speed,
        avg_peakgust
    FROM {{ ref('prep_weather_daily') }}
),
departures AS (
    SELECT
        origin AS faa,
        flight_date AS date,
        COUNT(DISTINCT dest) AS nunique_to,
        COUNT(sched_dep_time) AS dep_planned,
        SUM(cancelled) AS dep_cancelled,
        SUM(diverted) AS dep_diverted,
        SUM(CASE WHEN cancelled = 0 THEN 1 END) AS dep_n_flights_calc,
        COUNT(DISTINCT tail_number) AS tot_unique_tails_dep,
        COUNT(DISTINCT airline) AS tot_unique_aircompanies_dep
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, flight_date
),
arrivals AS (
    SELECT
        dest AS faa,
        flight_date AS date,
        COUNT(DISTINCT origin) AS nunique_from,
        COUNT(sched_arr_time) AS arr_planned,
        SUM(cancelled) AS arr_cancelled,
        SUM(diverted) AS arr_diverted,
        SUM(CASE WHEN cancelled = 0 THEN 1 END) AS arr_n_flights_calc,
        COUNT(DISTINCT tail_number) AS tot_unique_tails_arr,
        COUNT(DISTINCT airline) AS tot_unique_aircompanies_arr
    FROM {{ ref('prep_flights') }}
    GROUP BY dest, flight_date
),
total_stats AS (
    SELECT
        w.faa,
        w.date,
        w.min_temp_c,
        w.max_temp_c,
        w.precipitation_mm,
        w.max_snow_mm,
        w.avg_wind_direction,
        w.avg_wind_speed,
        w.avg_peakgust,
        d.nunique_to,
        a.nunique_from,
        COALESCE(d.dep_planned, 0) + COALESCE(a.arr_planned, 0) AS total_planned,
        COALESCE(d.dep_cancelled, 0) + COALESCE(a.arr_cancelled, 0) AS total_cancelled,
        COALESCE(d.dep_diverted, 0) + COALESCE(a.arr_diverted, 0) AS total_diverted,
        COALESCE(d.dep_n_flights_calc, 0) + COALESCE(a.arr_n_flights_calc, 0) AS total_traffic,
        d.tot_unique_tails_dep,
        a.tot_unique_tails_arr,
        d.tot_unique_aircompanies_dep,
        a.tot_unique_aircompanies_arr
    FROM weather_daily AS w
    LEFT JOIN departures AS d
        ON w.faa = d.faa
       AND w.date = d.date
    LEFT JOIN arrivals AS a
        ON w.faa = a.faa
       AND w.date = a.date
)
SELECT
    pa.city,
    pa.country,
    pa.name,
    ts.*
FROM total_stats AS ts
JOIN {{ ref('prep_airports') }} AS pa
    USING (faa)