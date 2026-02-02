{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r7']
) }}

select
  d.region_code,
  d.h3_r7,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  iff(s.region_code is null, false, true) as has_poi_areas,

  coalesce(s.poi_areas_cnt, 0) as poi_areas_cnt,
  coalesce(s.poi_xarea_m2_sum, 0) as poi_xarea_m2_sum,
  coalesce(s.poi_xarea_share, 0) as poi_xarea_share,
  coalesce(s.poi_areas_per_km2, 0) as poi_areas_per_km2,
  coalesce(s.poi_xarea_m2_per_km2, 0) as poi_xarea_m2_per_km2,

  coalesce(s.poi_amenity_cnt, 0)  as poi_amenity_cnt,
  coalesce(s.poi_shop_cnt, 0)     as poi_shop_cnt,
  coalesce(s.poi_tourism_cnt, 0)  as poi_tourism_cnt,
  coalesce(s.poi_office_cnt, 0)   as poi_office_cnt,
  coalesce(s.poi_leisure_cnt, 0)  as poi_leisure_cnt,
  coalesce(s.poi_sport_cnt, 0)    as poi_sport_cnt,
  coalesce(s.poi_building_cnt, 0) as poi_building_cnt,
  coalesce(s.poi_landuse_cnt, 0)  as poi_landuse_cnt,

  coalesce(s.poi_amenity_xarea_m2_sum, 0)  as poi_amenity_xarea_m2_sum,
  coalesce(s.poi_shop_xarea_m2_sum, 0)     as poi_shop_xarea_m2_sum,
  coalesce(s.poi_tourism_xarea_m2_sum, 0)  as poi_tourism_xarea_m2_sum,
  coalesce(s.poi_office_xarea_m2_sum, 0)   as poi_office_xarea_m2_sum,
  coalesce(s.poi_leisure_xarea_m2_sum, 0)  as poi_leisure_xarea_m2_sum,
  coalesce(s.poi_sport_xarea_m2_sum, 0)    as poi_sport_xarea_m2_sum,
  coalesce(s.poi_building_xarea_m2_sum, 0) as poi_building_xarea_m2_sum,
  coalesce(s.poi_landuse_xarea_m2_sum, 0)  as poi_landuse_xarea_m2_sum,

  coalesce(s.poi_amenity_xarea_m2_sum, 0)  / nullif(d.cell_area_m2, 0) as poi_amenity_xarea_share,
  coalesce(s.poi_shop_xarea_m2_sum, 0)     / nullif(d.cell_area_m2, 0) as poi_shop_xarea_share,
  coalesce(s.poi_tourism_xarea_m2_sum, 0)  / nullif(d.cell_area_m2, 0) as poi_tourism_xarea_share,
  coalesce(s.poi_office_xarea_m2_sum, 0)   / nullif(d.cell_area_m2, 0) as poi_office_xarea_share,
  coalesce(s.poi_leisure_xarea_m2_sum, 0)  / nullif(d.cell_area_m2, 0) as poi_leisure_xarea_share,
  coalesce(s.poi_sport_xarea_m2_sum, 0)    / nullif(d.cell_area_m2, 0) as poi_sport_xarea_share,
  coalesce(s.poi_building_xarea_m2_sum, 0) / nullif(d.cell_area_m2, 0) as poi_building_xarea_share,
  coalesce(s.poi_landuse_xarea_m2_sum, 0)  / nullif(d.cell_area_m2, 0) as poi_landuse_xarea_share,

  /* nullable for empty cells */
  s.last_load_ts
from {{ ref('dim_h3_r7_cells') }} d
left join {{ ref('feat_h3_poi_areas_r7_xarea_classbuckets') }} s
  on s.region_code = d.region_code
 and s.h3_r7       = d.h3_r7