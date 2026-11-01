-- Gold: airport x day x hour (UTC). The airport dimension is small: broadcast join (no shuffle).
WITH hourly AS (
    SELECT airport_icao, movement_date, movement_hour,
           SUM(CASE WHEN movement_type = 'DEP' THEN 1 ELSE 0 END) AS departures,
           SUM(CASE WHEN movement_type = 'ARR' THEN 1 ELSE 0 END) AS arrivals,
           COUNT(*)                                               AS movements
    FROM airport_movements
    GROUP BY airport_icao, movement_date, movement_hour
)
SELECT /*+ BROADCAST(a) */
       h.*, a.airport_name, a.iso_country, a.municipality,
       current_timestamp() AS _gold_built_at_utc
FROM hourly h
LEFT JOIN airports_dim a ON h.airport_icao = a.airport_icao
