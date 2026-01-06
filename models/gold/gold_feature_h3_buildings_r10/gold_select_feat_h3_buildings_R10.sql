SELECT
    region::STRING                      AS region,
    h3_r10::STRING                      AS h3_r10,
    LOWER(building_type::STRING)        AS building_type,
    COALESCE(building_levels::INT, 1)   AS building_levels,
    TRY_TO_GEOGRAPHY(geom_wkt_4326)     AS geog,
    load_ts::TIMESTAMP_NTZ              AS load_ts
FROM SILVER.BUILDING_FOOTPRINTS_MODEL
WHERE 
    h3_r10 IS NOT NULL
        AND geom_wkt_4326 IS NOT NULL