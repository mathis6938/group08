-- Gold: airport x runway x day, with the runway characteristics of OurAirports.
-- OSM runway references look like "07-25", "14L/32R" or "07": normalised to "LE-HE", then matched with the two
-- runway ends stored by OurAirports (le_ident, he_ident), in both orders.
WITH runway_movements AS (
    SELECT airport_icao, movement_date, movement_type,
           regexp_replace(upper(trim(runway_ref)), '\\s*[/;]\\s*|\\s+', '-') AS runway_ident
    FROM airport_movements
    WHERE runway_ref IS NOT NULL
),
counts AS (
    SELECT airport_icao, runway_ident, movement_date,
           SUM(CASE WHEN movement_type = 'DEP' THEN 1 ELSE 0 END) AS runway_departures,
           SUM(CASE WHEN movement_type = 'ARR' THEN 1 ELSE 0 END) AS runway_arrivals,
           COUNT(*)                                               AS runway_movements
    FROM runway_movements
    GROUP BY airport_icao, runway_ident, movement_date
),
runways AS (                                             -- one row per runway (duplicates in OurAirports removed)
    SELECT airport_icao, le_ident, he_ident, length_ft, width_ft, surface, is_closed,
           concat_ws('-', le_ident, he_ident) AS rw_key,
           concat_ws('-', he_ident, le_ident) AS rw_key_reversed
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY airport_icao, le_ident, he_ident
                                       ORDER BY length_ft DESC NULLS LAST) AS rn
          FROM silver_runways)
    WHERE rn = 1
)
SELECT /*+ BROADCAST(r) */
       c.*, r.length_ft, r.width_ft, r.surface, r.is_closed,
       current_timestamp() AS _gold_built_at_utc
FROM counts c
LEFT JOIN runways r
       ON c.airport_icao = r.airport_icao
      AND c.runway_ident IN (r.rw_key, r.rw_key_reversed, r.le_ident, r.he_ident)
