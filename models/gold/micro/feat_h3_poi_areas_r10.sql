{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with base as (
  select
    region_code::string as region_code,
    region::string as region,
    {{ h3_r10_from_geog_centroid('geog') }}::string as h3_r10,
    lower(poi_class)::string as poi_class,
    lower(poi_type)::string  as poi_type,
    load_ts::timestamp_ntz   as load_ts
  from {{ ref('poi_areas') }}
  where geog is not null
    and region_code is not null
    and region is not null
    and poi_class is not null
),

agg as (
  select
    region_code,
    region,
    h3_r10,

    count(*) as poi_areas_cnt,
    count_if(poi_class = 'amenity')  as amenity_areas_cnt,
    count_if(poi_class = 'shop')     as shop_areas_cnt,
    count_if(poi_class = 'tourism')  as tourism_areas_cnt,
    count_if(poi_class = 'office')   as office_areas_cnt,
    count_if(poi_class = 'leisure')  as leisure_areas_cnt,
    count_if(poi_class = 'sport')    as sport_areas_cnt,
    count_if(poi_class = 'building') as building_areas_cnt,
    count_if(poi_class = 'landuse')  as landuse_areas_cnt,

    max(load_ts) as last_load_ts
  from base
  where h3_r10 is not null
  group by 1,2,3 
),

cells as (
  select
    region_code::string as region_code,
    region::string as region,
    h3_r10::string      as h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and region is not null
    and h3_r10 is not null
)

select
  c.region_code,
  c.region,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  /* counts: 0 for empty cells */
  coalesce(a.poi_areas_cnt, 0)        as poi_areas_cnt,
  coalesce(a.amenity_areas_cnt, 0)    as amenity_areas_cnt,
  coalesce(a.shop_areas_cnt, 0)       as shop_areas_cnt,
  coalesce(a.tourism_areas_cnt, 0)    as tourism_areas_cnt,
  coalesce(a.office_areas_cnt, 0)     as office_areas_cnt,
  coalesce(a.leisure_areas_cnt, 0)    as leisure_areas_cnt,
  coalesce(a.sport_areas_cnt, 0)      as sport_areas_cnt,
  coalesce(a.building_areas_cnt, 0)   as building_areas_cnt,
  coalesce(a.landuse_areas_cnt, 0)    as landuse_areas_cnt,

  /* densities per km2 (hex area) */
  coalesce(a.poi_areas_cnt, 0)        / nullif(c.cell_area_m2 / 1e6, 0) as poi_areas_per_km2,
  coalesce(a.amenity_areas_cnt, 0)    / nullif(c.cell_area_m2 / 1e6, 0) as amenity_areas_per_km2,
  coalesce(a.shop_areas_cnt, 0)       / nullif(c.cell_area_m2 / 1e6, 0) as shop_areas_per_km2,
  coalesce(a.tourism_areas_cnt, 0)    / nullif(c.cell_area_m2 / 1e6, 0) as tourism_areas_per_km2,
  coalesce(a.office_areas_cnt, 0)     / nullif(c.cell_area_m2 / 1e6, 0) as office_areas_per_km2,
  coalesce(a.leisure_areas_cnt, 0)    / nullif(c.cell_area_m2 / 1e6, 0) as leisure_areas_per_km2,
  coalesce(a.sport_areas_cnt, 0)      / nullif(c.cell_area_m2 / 1e6, 0) as sport_areas_per_km2,

  /* NULL for empty cells is expected */
  a.last_load_ts
from cells c
left join agg a
  on a.region_code = c.region_code
 and a.region = c.region
 and a.h3_r10      = c.h3_r10