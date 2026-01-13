SELECT
  c.region_code,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.buildings_cnt,
  a.res_buildings_cnt,
  a.nonres_buildings_cnt,
  a.unknown_buildings_cnt,
  a.footprint_area_m2_sum,
  a.floor_area_m2_est_sum,
  a.levels_avg,
  a.levels_p50,
  a.footprint_area_p50_m2,
  a.footprint_area_p90_m2,
  a.buildings_cnt / NULLIF(c.cell_area_m2 / 1e6, 0)            AS buildings_per_km2,
  a.footprint_area_m2_sum / NULLIF(c.cell_area_m2 / 1e6, 0)    AS footprint_m2_per_km2,
  a.floor_area_m2_est_sum / NULLIF(c.cell_area_m2 / 1e6, 0)    AS floor_area_m2_per_km2,
  a.footprint_area_m2_sum / NULLIF(c.cell_area_m2, 0)          AS built_up_share,
  a.last_load_ts
FROM {{ ref('cell_area_ref_feat_h3_buildings_R10') }} c
JOIN {{ ref('agg_levels_area_count_ref_foot_area') }} a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10