{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with base as (
  select
    region_code,
    {{ h3_r10_from_geog_centroid('geog') }} as h3_r10,
    lower(poi_class)::string as poi_class,
    lower(poi_type)::string  as poi_type,
    load_ts
  from {{ ref('poi_areas') }}
  where geog is not null
    and region_code is not null
    and poi_class is not null
),

agg as (
  select
    region_code,
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
  group by 1,2
),

final as (
  select
    a.region_code,
    a.h3_r10,
    d.cell_area_m2,
    d.cell_wkt_4326,
    d.cell_center_wkt_4326,
    a.poi_areas_cnt,
    a.amenity_areas_cnt,
    a.shop_areas_cnt,
    a.tourism_areas_cnt,
    a.office_areas_cnt,
    a.leisure_areas_cnt,
    a.sport_areas_cnt,
    a.building_areas_cnt,
    a.landuse_areas_cnt,
    /* densities per km2 */
    a.poi_areas_cnt / nullif(d.cell_area_m2 / 1e6, 0) as poi_areas_per_km2,
    a.amenity_areas_cnt / nullif(d.cell_area_m2 / 1e6, 0) as amenity_areas_per_km2,
    a.shop_areas_cnt    / nullif(d.cell_area_m2 / 1e6, 0) as shop_areas_per_km2,
    a.tourism_areas_cnt / nullif(d.cell_area_m2 / 1e6, 0) as tourism_areas_per_km2,
    a.office_areas_cnt  / nullif(d.cell_area_m2 / 1e6, 0) as office_areas_per_km2,
    a.leisure_areas_cnt / nullif(d.cell_area_m2 / 1e6, 0) as leisure_areas_per_km2,
    a.sport_areas_cnt   / nullif(d.cell_area_m2 / 1e6, 0) as sport_areas_per_km2,
    a.last_load_ts
  from agg a
  join {{ ref('dim_h3_r10_cells') }} d
    on d.region_code = a.region_code
   and d.h3_r10 = a.h3_r10
)

select * from final