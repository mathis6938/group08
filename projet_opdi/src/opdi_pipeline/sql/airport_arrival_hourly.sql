-- Gold: hourly arrival demand, measured arrivals, additional time, holdings and completeness.
WITH matched_arrivals AS (
    SELECT a.dest_icao,
           a.arrival_date,
           a.arrival_hour,
           a.is_transit_plausible,
           a.level_minutes,
           a.holding_count,
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
hourly_aggs AS (
    SELECT dest_icao AS airport_icao,
           arrival_date AS movement_date,
           arrival_hour AS movement_hour,
           COUNT(*)                                                           AS arrivals,
           SUM(CASE WHEN is_transit_plausible THEN 1 ELSE 0 END)              AS arrivals_measured,
           ROUND(AVG(CASE WHEN is_transit_plausible THEN additional_min END), 2) AS avg_additional_min,
           ROUND(AVG(level_minutes), 2)                                       AS avg_level_minutes,
           SUM(holding_count)                                                 AS holdings
    FROM matched_arrivals
    GROUP BY dest_icao, arrival_date, arrival_hour
)
SELECT h.airport_icao,
       h.movement_date,
       h.movement_hour,
       h.arrivals,
       h.arrivals_measured,
       h.avg_additional_min,
       h.avg_level_minutes,
       h.holdings,
       COALESCE(c.is_complete_hour, true) AS is_complete_hour,
       current_timestamp()                AS _gold_built_at_utc
FROM hourly_aggs h
LEFT JOIN hourly_completeness c
       ON h.movement_date = c.movement_date
      AND h.movement_hour = c.movement_hour
