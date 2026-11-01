-- One row per airport movement: a flight gives a departure (DEP) at its origin and an arrival (ARR) at its
-- destination. Movement time: runway event, else take-off / landing event, else first_seen / last_seen.
WITH movements AS (
    SELECT flight_id,
           origin_icao                                          AS airport_icao,
           'DEP'                                                AS movement_type,
           COALESCE(dep_runway_utc, takeoff_utc, first_seen_utc) AS movement_time_utc,
           CASE WHEN dep_runway_utc IS NOT NULL THEN 'runway event'
                WHEN takeoff_utc    IS NOT NULL THEN 'take-off / landing event'
                ELSE 'first / last seen' END                    AS time_source,
           (dep_runway_utc IS NOT NULL OR takeoff_utc IS NOT NULL) AS time_from_event,
           dep_runway_ref                                       AS runway_ref,
           taxi_out_min                                         AS taxi_min,
           operator_icao, aircraft_typecode, aircraft_class, flight_category
    FROM flights_timed
    WHERE origin_icao IS NOT NULL
    UNION ALL
    SELECT flight_id,
           dest_icao,
           'ARR',
           COALESCE(arr_runway_utc, landing_utc, last_seen_utc),
           CASE WHEN arr_runway_utc IS NOT NULL THEN 'runway event'
                WHEN landing_utc    IS NOT NULL THEN 'take-off / landing event'
                ELSE 'first / last seen' END,
           (arr_runway_utc IS NOT NULL OR landing_utc IS NOT NULL),
           arr_runway_ref,
           taxi_in_min,
           operator_icao, aircraft_typecode, aircraft_class, flight_category
    FROM flights_timed
    WHERE dest_icao IS NOT NULL
)
SELECT *,
       to_date(movement_time_utc) AS movement_date,
       hour(movement_time_utc)    AS movement_hour,
       -- ICAO aircraft class, e.g. "L2J" = landplane, 2 engines, Jet: the 3rd character is the engine type
       CASE WHEN aircraft_class IS NOT NULL THEN substr(aircraft_class, 3, 1) = 'J' END AS is_jet
FROM movements
WHERE movement_time_utc >= CAST(:scope_start AS TIMESTAMP)
  AND movement_time_utc <  CAST(:scope_end   AS TIMESTAMP)
