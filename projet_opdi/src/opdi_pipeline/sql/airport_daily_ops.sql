-- Gold: airport x day. Averages ignore NULL: an unknown aircraft class or country is left out of the shares.
WITH daily AS (
    SELECT airport_icao, movement_date,
           SUM(CASE WHEN movement_type = 'DEP' THEN 1 ELSE 0 END)                        AS departures,
           SUM(CASE WHEN movement_type = 'ARR' THEN 1 ELSE 0 END)                        AS arrivals,
           COUNT(*)                                                                      AS movements,
           COUNT(DISTINCT operator_icao)                                                 AS distinct_operators,
           COUNT(DISTINCT aircraft_typecode)                                             AS distinct_aircraft_types,
           ROUND(AVG(CAST(is_jet AS INT)) * 100, 1)                                      AS pct_jet,
           ROUND(AVG(CASE WHEN flight_category <> 'UNKNOWN'
                          THEN CAST(flight_category = 'INTERNATIONAL' AS INT) END) * 100, 1) AS pct_international,
           ROUND(AVG(CAST(time_from_event AS INT)) * 100, 1)                             AS pct_time_from_event
    FROM airport_movements
    GROUP BY airport_icao, movement_date
),
peak AS (
    SELECT airport_icao, movement_date, MAX(movements) AS peak_hour_movements
    FROM (SELECT airport_icao, movement_date, movement_hour, COUNT(*) AS movements
          FROM airport_movements
          GROUP BY airport_icao, movement_date, movement_hour)
    GROUP BY airport_icao, movement_date
)
SELECT /*+ BROADCAST(a) */
       d.*, p.peak_hour_movements,
       a.airport_name, a.iso_country, a.municipality,
       dayofweek(d.movement_date) AS day_of_week,          -- 1 = Sunday ... 7 = Saturday
       current_timestamp()        AS _gold_built_at_utc
FROM daily d
LEFT JOIN peak p ON d.airport_icao = p.airport_icao AND d.movement_date = p.movement_date
LEFT JOIN airports_dim a ON d.airport_icao = a.airport_icao
