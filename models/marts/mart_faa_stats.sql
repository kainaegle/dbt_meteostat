WITH departures AS (
SELECT 
origin,
COUNT(DISTINCT dest) AS nunique_to, -- unique number of departures connections
COUNT(sched_dep_time) AS dep_planned, -- how many flight were planned in total (departures & arrivals)
SUM(cancelled) AS dep_cancelled, -- how many flights were canceled in total (departures & arrivals)
SUM(diverted) AS dep_diverted, -- how many flights were diverted in total (departures & arrivals)
SUM(CASE WHEN cancelled = 0 THEN 1 END) AS dep_n_flights_calc, -- how many flights actually occured in total (departures & arrivals)
COUNT(DISTINCT tail_number) AS tot_unique_tails_dep, -- how many unique airplanes
COUNT(DISTINCT airline) AS tot_unique_aircompanies_dep -- how many unique airlines were in service 
-- FROM prep_flights
FROM{{ref('prep_flights')}}
GROUP BY origin
),
arrivals AS (
SELECT 
dest, 
COUNT(DISTINCT origin) AS nunique_from, -- unique number of arrival connections
COUNT(sched_arr_time) AS arr_planned, -- how many flight were planned in total (departures & arrivals)
SUM(cancelled) AS arr_cancelled, -- how many flights were canceled in total (departures & arrivals)
SUM(diverted) AS arr_diverted, -- how many flights were diverted in total (departures & arrivals)
SUM(CASE WHEN cancelled = 0 THEN 1 END) AS arr_n_flights_calc, -- how many flights actually occured in total (departures & arrivals)
COUNT(DISTINCT tail_number) AS tot_unique_tails_arr, -- how many unique airplanes
COUNT(DISTINCT airline) AS tot_unique_aircompanies_arr -- how many unique airlines were in service 
--FROM prep_flights
FROM{{ref('prep_flights')}}
GROUP BY dest
),
total_stats AS (
SELECT 
origin AS faa,
nunique_to,
nunique_from,
dep_planned + arr_planned AS total_planned,
dep_cancelled + arr_cancelled AS total_cancelled,
dep_diverted + arr_diverted AS total_diverted,
dep_n_flights_calc + arr_n_flights_calc AS total_traffic,
tot_unique_tails_dep,
tot_unique_tails_arr,
tot_unique_aircompanies_dep,
tot_unique_aircompanies_arr
FROM departures
JOIN arrivals
ON origin = dest
)
SELECT pa.city, pa.country, pa.name, ts.*
FROM total_stats AS ts 
JOIN {{ref('prep_airports')}}

