-- Gold: daily arrival performance by airport (excluding incomplete traffic hours).
WITH matched_arrivals AS (
    SELECT a.dest_icao,
           a.arrival_date,
           a.arrival_hour,
           a.is_transit_plausible,
           a.level_off_count,
           a.level_minutes,
           a.level_minutes_lt_fl70,
           a.level_minutes_fl70_100,
           a.level_minutes_fl100_200,
           a.level_minutes_ge_fl200,
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
filtered_arrivals AS (
    SELECT m.*
    FROM matched_arrivals m
    LEFT JOIN hourly_completeness c
           ON m.arrival_date = c.movement_date
          AND m.arrival_hour = c.movement_hour
    WHERE COALESCE(c.is_complete_hour, true) = true
),
daily_aggs AS (
    SELECT dest_icao AS airport_icao,
           arrival_date AS movement_date,
           COUNT(*)                                                                     AS arrivals,
           SUM(CASE WHEN is_transit_plausible THEN 1 ELSE 0 END)                        AS arrivals_measured,
           ROUND(SUM(CASE WHEN is_transit_plausible THEN 1 ELSE 0 END) * 100.0D / COUNT(*), 1) AS pct_coverage,
           ROUND(AVG(CASE WHEN is_transit_plausible THEN additional_min END), 2)        AS avg_additional_min,
           ROUND(percentile_approx(CASE WHEN is_transit_plausible THEN additional_min END, 0.50D, 1000), 2) AS median_additional_min,
           ROUND(percentile_approx(CASE WHEN is_transit_plausible THEN additional_min END, 0.90D, 1000), 2) AS p90_additional_min,
           ROUND(SUM(CASE WHEN is_transit_plausible THEN additional_min END), 1)        AS total_additional_min,
           ROUND(AVG(level_off_count), 2)                                               AS avg_level_offs,
           ROUND(AVG(level_minutes), 2)                                                 AS avg_level_minutes,
           ROUND(SUM(level_minutes_lt_fl70), 1)                                         AS level_minutes_lt_fl70,
           ROUND(SUM(level_minutes_fl70_100), 1)                                        AS level_minutes_fl70_100,
           ROUND(SUM(level_minutes_fl100_200), 1)                                       AS level_minutes_fl100_200,
           ROUND(SUM(level_minutes_ge_fl200), 1)                                        AS level_minutes_ge_fl200,
           ROUND(AVG(CASE WHEN holding_count > 0 THEN 1 ELSE 0 END) * 100.0D, 1)        AS pct_with_holding,
           ROUND(SUM(holding_minutes), 1)                                               AS holding_minutes,
           ROUND(SUM(level_minutes) * :fuel_coef, 1)                                    AS est_fuel_kg,
           ROUND(SUM(level_minutes) * :fuel_coef * :co2_factor, 1)                       AS est_co2_kg
    FROM filtered_arrivals
    GROUP BY dest_icao, arrival_date
)
SELECT /*+ BROADCAST(a) */
       d.*,
       a.airport_name,
       a.iso_country,
       a.municipality,
       current_timestamp() AS _gold_built_at_utc
FROM daily_aggs d
LEFT JOIN airports_dim a ON d.airport_icao = a.airport_icao
