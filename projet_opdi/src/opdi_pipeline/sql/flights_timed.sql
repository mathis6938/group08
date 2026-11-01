-- Airport milestones of each flight, from its take-off / landing / runway / parking events.
-- Airport events give their airport in info['osm_airport'] (ICAO code) and the airport element in
-- info['osm_ref'] (e.g. runway "07-25"): each event is attached to the departure or to the arrival airport.
WITH ground_events AS (
    SELECT e.flight_id,
           e.event_type,
           e.event_timestamp_utc  AS t,
           e.info['osm_airport']  AS event_airport,
           e.info['osm_ref']      AS element_ref,
           f.origin_icao,
           f.dest_icao
    FROM silver_events e
    JOIN silver_flights f ON e.flight_id = f.flight_id
    WHERE e.event_type IN ('take-off', 'landing', 'entry-runway', 'exit-runway',
                           'exit-parking_position', 'entry-parking_position')
),
milestones AS (
    SELECT flight_id,
           MIN(CASE WHEN event_type = 'take-off' THEN t END) AS takeoff_utc,
           MAX(CASE WHEN event_type = 'landing'  THEN t END) AS landing_utc,
           -- (time, runway) of the last runway entry at the departure airport = line-up before take-off
           MAX(CASE WHEN event_type = 'entry-runway' AND event_airport = origin_icao
                    THEN named_struct('t', t, 'ref', element_ref) END) AS dep_runway,
           -- (time, runway) of the first runway exit at the arrival airport = runway vacated after landing
           MIN(CASE WHEN event_type = 'exit-runway' AND event_airport = dest_icao
                    THEN named_struct('t', t, 'ref', element_ref) END) AS arr_runway,
           MAX(CASE WHEN event_type = 'exit-parking_position'  AND event_airport = origin_icao THEN t END)
               AS exit_parking_utc,
           MIN(CASE WHEN event_type = 'entry-parking_position' AND event_airport = dest_icao   THEN t END)
               AS entry_parking_utc
    FROM ground_events
    GROUP BY flight_id
),
timed AS (
    SELECT f.flight_id, f.origin_icao, f.dest_icao, f.operator_icao, f.aircraft_typecode, f.aircraft_class,
           f.flight_category, f.first_seen_utc, f.last_seen_utc,
           m.takeoff_utc, m.landing_utc,
           m.dep_runway.t   AS dep_runway_utc,
           m.dep_runway.ref AS dep_runway_ref,
           m.arr_runway.t   AS arr_runway_utc,
           m.arr_runway.ref AS arr_runway_ref,
           m.exit_parking_utc, m.entry_parking_utc,
           ROUND((unix_timestamp(m.dep_runway.t) - unix_timestamp(m.exit_parking_utc)) / 60.0D, 1) AS taxi_out_raw,
           ROUND((unix_timestamp(m.entry_parking_utc) - unix_timestamp(m.arr_runway.t)) / 60.0D, 1) AS taxi_in_raw
    FROM silver_flights f
    LEFT JOIN milestones m ON f.flight_id = m.flight_id
)
SELECT flight_id, origin_icao, dest_icao, operator_icao, aircraft_typecode, aircraft_class, flight_category,
       first_seen_utc, last_seen_utc, takeoff_utc, landing_utc, dep_runway_utc, dep_runway_ref,
       arr_runway_utc, arr_runway_ref, exit_parking_utc, entry_parking_utc,
       -- outside [1, 120] minutes: not a plausible taxi time
       CASE WHEN taxi_out_raw BETWEEN 1 AND 120 THEN taxi_out_raw END AS taxi_out_min,
       CASE WHEN taxi_in_raw  BETWEEN 1 AND 120 THEN taxi_in_raw  END AS taxi_in_min
FROM timed
