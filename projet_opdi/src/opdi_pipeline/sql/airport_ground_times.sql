-- Gold: airport x day, taxi-out (parking -> departure runway) and taxi-in (arrival runway -> parking):
-- mean, median, 90th percentile, and coverage = share of the movements with a measured taxi time.
WITH taxi_out AS (
    SELECT airport_icao, movement_date,
           COUNT(*)                                     AS total_departures,
           COUNT(taxi_min)                              AS taxi_out_measured_flights,
           ROUND(AVG(taxi_min), 1)                      AS avg_taxi_out_min,
           ROUND(percentile_approx(taxi_min, 0.5), 1)   AS median_taxi_out_min,
           ROUND(percentile_approx(taxi_min, 0.9), 1)   AS p90_taxi_out_min
    FROM airport_movements
    WHERE movement_type = 'DEP'
    GROUP BY airport_icao, movement_date
),
taxi_in AS (
    SELECT airport_icao, movement_date,
           COUNT(*)                                     AS total_arrivals,
           COUNT(taxi_min)                              AS taxi_in_measured_flights,
           ROUND(AVG(taxi_min), 1)                      AS avg_taxi_in_min,
           ROUND(percentile_approx(taxi_min, 0.5), 1)   AS median_taxi_in_min,
           ROUND(percentile_approx(taxi_min, 0.9), 1)   AS p90_taxi_in_min
    FROM airport_movements
    WHERE movement_type = 'ARR'
    GROUP BY airport_icao, movement_date
)
SELECT /*+ BROADCAST(a) */
       t.*, a.airport_name, a.iso_country, a.municipality,
       CASE WHEN t.total_departures > 0
            THEN ROUND(t.taxi_out_measured_flights / t.total_departures * 100, 1)
            ELSE CAST(0 AS DOUBLE) END AS pct_taxi_out_coverage,
       CASE WHEN t.total_arrivals > 0
            THEN ROUND(t.taxi_in_measured_flights / t.total_arrivals * 100, 1)
            ELSE CAST(0 AS DOUBLE) END AS pct_taxi_in_coverage,
       current_timestamp() AS _gold_built_at_utc
FROM (SELECT *                                  -- airport-days with departures and / or arrivals
      FROM taxi_out
      FULL OUTER JOIN taxi_in USING (airport_icao, movement_date)) t
LEFT JOIN airports_dim a ON t.airport_icao = a.airport_icao
