WITH route_stats AS (
    SELECT 
        origin, -- origin airport code
        dest, -- destination airport code
        COUNT(*) AS total_flights, -- total flights on this route
        COUNT(DISTINCT tail_number) AS tot_unique_tails, -- unique airplanes
        COUNT(DISTINCT airline) AS tot_unique_aircompanies, -- unique airlines
        AVG(actual_elapsed_time) AS avg_actual_elapsed_time, -- on average what is the actual elapsed time
        AVG(arr_delay) AS avg_arr_delay, -- on average what is the delay on arrival
        MAX(arr_delay) AS max_arr_delay, -- what was the max delay?
        MIN(arr_delay) AS min_arr_delay, -- what was the min delay?
        SUM(cancelled) AS total_cancelled, -- total number of cancelled
        SUM(diverted) AS total_diverted -- total number of diverted
        FROM {{ ref('prep_flights') }}
        GROUP BY origin, dest
)
SELECT 
po.city AS origin_city,
po.country AS origin_country,
po.name AS origin_name,
pd.city AS dest_city,
pd.country AS dest_country,
pd.name AS dest_name,
rs.*
FROM route_stats AS rs
JOIN {{ ref('prep_airports') }} AS po
    ON rs.origin = po.faa
JOIN {{ ref('prep_airports') }} AS pd
    ON rs.dest = pd.faa