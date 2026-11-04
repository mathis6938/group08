-- Reference arrival transit time: 20th percentile of transit time by
-- (dest_icao, arrival_runway, aircraft_class, entry_sector) with fallback hierarchy:
-- 1. RUNWAY_CLASS_SECTOR (>= 20 flights)
-- 2. CLASS_SECTOR (>= 20 flights)
-- 3. AIRPORT (>= 1 flight)
WITH plausible AS (
    SELECT dest_icao, arrival_runway, aircraft_class, entry_sector, transit_min
    FROM silver_arrivals
    WHERE is_transit_plausible = true
),
l1 AS (
    SELECT dest_icao AS airport_icao,
           arrival_runway,
           aircraft_class,
           entry_sector,
           'RUNWAY_CLASS_SECTOR'                                  AS fallback_level,
           COUNT(*)                                               AS n_flights,
           ROUND(percentile_approx(transit_min, 0.20D, 1000), 2)  AS ref_transit_min
    FROM plausible
    WHERE arrival_runway IS NOT NULL
      AND aircraft_class IS NOT NULL
      AND entry_sector IS NOT NULL
    GROUP BY dest_icao, arrival_runway, aircraft_class, entry_sector
    HAVING COUNT(*) >= 20
),
l2 AS (
    SELECT dest_icao AS airport_icao,
           CAST(NULL AS STRING)                                   AS arrival_runway,
           aircraft_class,
           entry_sector,
           'CLASS_SECTOR'                                         AS fallback_level,
           COUNT(*)                                               AS n_flights,
           ROUND(percentile_approx(transit_min, 0.20D, 1000), 2)  AS ref_transit_min
    FROM plausible
    WHERE aircraft_class IS NOT NULL
      AND entry_sector IS NOT NULL
    GROUP BY dest_icao, aircraft_class, entry_sector
    HAVING COUNT(*) >= 20
),
l3 AS (
    SELECT dest_icao AS airport_icao,
           CAST(NULL AS STRING)                                   AS arrival_runway,
           CAST(NULL AS STRING)                                   AS aircraft_class,
           CAST(NULL AS STRING)                                   AS entry_sector,
           'AIRPORT'                                              AS fallback_level,
           COUNT(*)                                               AS n_flights,
           ROUND(percentile_approx(transit_min, 0.20D, 1000), 2)  AS ref_transit_min
    FROM plausible
    GROUP BY dest_icao
    HAVING COUNT(*) >= 1
),
unioned AS (
    SELECT * FROM l1
    UNION ALL
    SELECT * FROM l2
    UNION ALL
    SELECT * FROM l3
)
SELECT airport_icao, arrival_runway, aircraft_class, entry_sector, fallback_level,
       n_flights, ref_transit_min,
       current_timestamp() AS _gold_built_at_utc
FROM unioned
