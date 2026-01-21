{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with poi as (
  select
    region_code::string    as region_code,
    feature_id::string     as feature_id,
    poi_class::string      as poi_class,
    poi_type::string       as poi_type,
    geog                   as poi_geog,
    {{ h3_r10_from_geog_centroid('geog') }}::string as h3_r10,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('poi_areas') }}
  where geog is not null
    and region_code is not null
    and feature_id is not null
    and poi_class is not null
    and poi_type is not null
),

cells as (
  select
    region_code::string as region_code,
    h3_r10::string      as h3_r10,
    cell_geog           as cell_geog,
    cell_area_m2::float as cell_area_m2,
    cell_wkt_4326::string        as cell_wkt_4326,
    cell_center_wkt_4326::string as cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and h3_r10 is not null
    and cell_geog is not null
    and cell_area_m2 is not null
    and cell_area_m2 > 0
),

joined as (
  select
    c.region_code,
    c.h3_r10,
    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,
    p.feature_id,
    p.poi_class,
    p.poi_type,
    p.poi_geog,
    c.cell_geog,
    p.load_ts
  from poi p
  join cells c
    on c.region_code = p.region_code
   and c.h3_r10      = p.h3_r10
  where st_intersects(p.poi_geog, c.cell_geog)
),

intersections as (
  select
    region_code,
    h3_r10,
    feature_id,
    poi_class,
    poi_type,
    st_area(st_intersection(poi_geog, cell_geog))::float as poi_area_m2,
    load_ts,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from joined
  where poi_geog is not null
),

agg as (
  select
    region_code,
    h3_r10,
    any_value(cell_area_m2)         as cell_area_m2,
    any_value(cell_wkt_4326)        as cell_wkt_4326,
    any_value(cell_center_wkt_4326) as cell_center_wkt_4326,
    count(distinct feature_id) as poi_areas_cnt,
    sum(poi_area_m2) as poi_area_m2_sum,
    sum(poi_area_m2) / nullif(any_value(cell_area_m2), 0) as poi_area_share,
    /* counts by class */
    count(distinct iff(poi_class='amenity',  feature_id, null)) as amenity_areas_cnt,
    count(distinct iff(poi_class='shop',     feature_id, null)) as shop_areas_cnt,
    count(distinct iff(poi_class='tourism',  feature_id, null)) as tourism_areas_cnt,
    count(distinct iff(poi_class='office',   feature_id, null)) as office_areas_cnt,
    count(distinct iff(poi_class='leisure',  feature_id, null)) as leisure_areas_cnt,
    count(distinct iff(poi_class='sport',    feature_id, null)) as sport_areas_cnt,
    count(distinct iff(poi_class='building', feature_id, null)) as building_areas_cnt,
    count(distinct iff(poi_class='landuse',  feature_id, null)) as landuse_areas_cnt,
    /* area by class */
    sum(iff(poi_class='amenity',  poi_area_m2, 0)) as amenity_area_m2_sum,
    sum(iff(poi_class='shop',     poi_area_m2, 0)) as shop_area_m2_sum,
    sum(iff(poi_class='tourism',  poi_area_m2, 0)) as tourism_area_m2_sum,
    sum(iff(poi_class='office',   poi_area_m2, 0)) as office_area_m2_sum,
    sum(iff(poi_class='leisure',  poi_area_m2, 0)) as leisure_area_m2_sum,
    sum(iff(poi_class='sport',    poi_area_m2, 0)) as sport_area_m2_sum,
    sum(iff(poi_class='building', poi_area_m2, 0)) as building_area_m2_sum,
    sum(iff(poi_class='landuse',  poi_area_m2, 0)) as landuse_area_m2_sum,
    /* densities per km2 */
    count(distinct feature_id) / nullif(any_value(cell_area_m2)/1e6, 0) as poi_areas_per_km2,
    sum(poi_area_m2)           / nullif(any_value(cell_area_m2)/1e6, 0) as poi_area_m2_per_km2,
    max(load_ts) as last_load_ts
  from intersections
  where poi_area_m2 is not null
    and poi_area_m2 > 0
  group by 1,2
)

select * from agg