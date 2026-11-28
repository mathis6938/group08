-- Gold: arrival performance of each airport by weather category of the arrival hour (complete traffic hours, airports
-- with METAR, hours with at least one METAR). Same indicators as airport_arrival_*: additional time = transit time
-- minus the unimpeded reference (3-level fallback) for the measured arrivals; level-offs and holdings for all.
WITH matched_arrivals AS (
    SELECT a.dest_icao,
           a.arrival_date,
           a.arrival_hour,
           a.is_transit_plausible,
           a.level_minutes,
           a.holding_count,
           a.holding_minutes,
           CASE WHEN a.is_transit_plausible THEN
                a.transit_min - COALESCE(r1.ref_transit_min, r2.ref_transit_min, r3.ref_transit_min)
           END AS additional_min
    FROM silver_arrivals a
    LEFT JOIN gold_arrival_reference r1
           ON a.dest_icao = r1.airport_icao
          AND a.arrival_runway = r1.arrival_runway
          AND a.aircraft_class = r1.aircraft_class
          AND a.entry_sector = r1.entry_sector
          AND r1.fallback_level = 'RUNWAY_CLASS_SECTOR'
    LEFT JOIN gold_arrival_reference r2
           ON a.dest_icao = r2.airport_icao
          AND a.aircraft_class = r2.aircraft_class
          AND a.entry_sector = r2.entry_sector
          AND r2.fallback_level = 'CLASS_SECTOR'
    LEFT JOIN gold_arrival_reference r3
           ON a.dest_icao = r3.airport_icao
          AND r3.fallback_level = 'AIRPORT'
),
arrivals_weather AS (
    SELECT m.*, w.weather_category
    FROM matched_arrivals m
    JOIN hourly_completeness c
      ON m.arrival_date = c.movement_date
     AND m.arrival_hour = c.movement_hour
     AND c.is_complete_hour = true
    JOIN gold_airport_weather_hourly w
      ON m.dest_icao = w.airport_icao
     AND m.arrival_date = w.weather_date
     AND m.arrival_hour = w.weather_hour
),
impact AS (
    SELECT dest_icao                                                                    AS airport_icao,
           weather_category,
           COUNT(DISTINCT arrival_date, arrival_hour)                                   AS hours,
           COUNT(*)                                                                     AS arrivals,
           SUM(CASE WHEN is_transit_plausible THEN 1 ELSE 0 END)                        AS arrivals_measured,
           ROUND(AVG(CASE WHEN is_transit_plausible THEN additional_min END), 2)        AS avg_additional_min,
           ROUND(percentile_approx(CASE WHEN is_transit_plausible THEN additional_min END, 0.50D, 1000), 2) AS median_additional_min,
           ROUND(AVG(level_minutes), 2)                                                 AS avg_level_minutes,
           ROUND(AVG(CASE WHEN holding_count > 0 THEN 1 ELSE 0 END) * 100.0D, 1)        AS pct_with_holding,
           ROUND(SUM(holding_minutes), 1)                                               AS holding_minutes
    FROM arrivals_weather
    GROUP BY dest_icao, weather_category
)
SELECT /*+ BROADCAST(a) */
       i.*,
       a.airport_name,
       a.iso_country,
       current_timestamp() AS _gold_built_at_utc
FROM impact i
LEFT JOIN airports_dim a ON i.airport_icao = a.airport_icao
