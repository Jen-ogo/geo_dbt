SELECT
    region_code::STRING                  AS region_code,
    h3_r10::STRING                       AS h3_r10,
    LOWER(building_type::STRING)         AS building_type,
    COALESCE(building_levels::INT, 1)    AS building_levels,
    geog                                 AS geog,
    load_ts::TIMESTAMP_NTZ               AS load_ts
  FROM SILVER.BUILDING_FOOTPRINTS_MODEL
  WHERE h3_r10 IS NOT NULL
    AND geog IS NOT NULL