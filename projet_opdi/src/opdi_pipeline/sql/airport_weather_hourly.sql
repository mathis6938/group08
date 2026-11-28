-- Gold: weather of each airport hour, from the METAR of that hour (one every 30 or 60 minutes): worst visibility and
-- ceiling, strongest wind or gust, any thunderstorm / precipitation / fog / IMC / below CAT I / strong wind, and
-- one weather category per hour, the most severe condition first. Mean wind direction: vector mean weighted by
-- the wind speed (a plain mean of 350 deg and 10 deg would give 180 deg). Hours without METAR have no row.
WITH hourly AS (
    SELECT station_icao                                                   AS airport_icao,
           obs_date                                                       AS weather_date,
           obs_hour                                                       AS weather_hour,
           COUNT(*)                                                       AS observations,
           ROUND(AVG(temp_c), 1)                                          AS avg_temp_c,
           ROUND(AVG(wind_speed_kt), 1)                                   AS avg_wind_speed_kt,
           CASE WHEN SUM(CASE WHEN wind_dir_deg IS NOT NULL THEN wind_speed_kt END) > 0 THEN
                PMOD(ROUND(PMOD(DEGREES(ATAN2(SUM(wind_speed_kt * SIN(RADIANS(wind_dir_deg))),
                                              SUM(wind_speed_kt * COS(RADIANS(wind_dir_deg))))), 360.0D), 0), 360.0D)
           END                                                            AS wind_dir_deg,
           ROUND(MAX(GREATEST(wind_gust_kt, wind_speed_kt)), 1)           AS max_gust_kt,
           MIN(visibility_m)                                              AS min_visibility_m,
           MIN(ceiling_ft)                                                AS min_ceiling_ft,
           MAX(CAST(has_thunderstorm AS INT)) = 1                         AS has_thunderstorm,
           MAX(CAST(has_precipitation AS INT)) = 1                        AS has_precipitation,
           MAX(CAST(has_fog AS INT)) = 1                                  AS has_fog,
           MAX(CAST(is_imc AS INT)) = 1                                   AS is_imc,
           MAX(CAST(is_below_cat1 AS INT)) = 1                            AS is_below_cat1,
           MAX(CAST(is_strong_wind AS INT)) = 1                           AS is_strong_wind
    FROM silver_weather_observations
    GROUP BY station_icao, obs_date, obs_hour
)
SELECT h.*,
       CASE WHEN has_thunderstorm THEN 'THUNDERSTORM'
            WHEN is_below_cat1    THEN 'LOW_VISIBILITY'
            WHEN is_imc           THEN 'IMC'
            WHEN is_strong_wind   THEN 'STRONG_WIND'
            WHEN has_precipitation THEN 'PRECIPITATION'
            ELSE 'GOOD' END                                               AS weather_category,
       current_timestamp()                                                AS _gold_built_at_utc
FROM hourly h
