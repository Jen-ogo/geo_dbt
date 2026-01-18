SELECT
    region_code,
    h3_r10,
    COUNT(*)                              AS buildings_cnt,
    COUNT_IF(building_group = 'residential')    AS res_buildings_cnt,
    COUNT_IF(building_group = 'nonresidential') AS nonres_buildings_cnt,
    COUNT_IF(building_group = 'unknown')        AS unknown_buildings_cnt,
    SUM(footprint_area_m2)                AS footprint_area_m2_sum,
    SUM(footprint_area_m2 * building_levels) AS floor_area_m2_est_sum,
    AVG(building_levels)::FLOAT           AS levels_avg,
    APPROX_PERCENTILE(building_levels, 0.5) AS levels_p50,
    APPROX_PERCENTILE(footprint_area_m2, 0.5) AS footprint_area_p50_m2,
    APPROX_PERCENTILE(footprint_area_m2, 0.9) AS footprint_area_p90_m2,
    MAX(load_ts)                          AS last_load_ts
  FROM {{ ref('footprint_area_ref_h3') }}
  GROUP BY 1,2