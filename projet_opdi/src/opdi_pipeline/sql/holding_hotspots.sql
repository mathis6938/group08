-- Gold: holding hotspots by airport, 0.1 degree lat/lon grid, and altitude band.
SELECT dest_icao                   AS airport_icao,
       ROUND(start_lat, 1)         AS lat_cell,
       ROUND(start_lon, 1)         AS lon_cell,
       altitude_band,
       COUNT(*)                    AS holdings,
       ROUND(SUM(duration_min), 1) AS holding_minutes,
       current_timestamp()         AS _gold_built_at_utc
FROM silver_descent_level_offs
WHERE is_holding = true
GROUP BY dest_icao, ROUND(start_lat, 1), ROUND(start_lon, 1), altitude_band
