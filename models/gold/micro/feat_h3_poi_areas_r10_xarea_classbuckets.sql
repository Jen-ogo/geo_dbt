{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with base as (
  select
    p.region_code::string as region_code,
    p.region::string as region,
    p.feature_id::string  as feature_id,
    p.poi_class::string   as poi_class,
    p.poi_type::string    as poi_type,
    p.geog                as poi_geog,
    {{ h3_r10_from_geog_centroid('p.geog') }}::string as h3_r10,
    p.load_ts::timestamp_ntz as load_ts
  from {{ ref('poi_areas') }} p
  where p.geog is not null
    and p.region_code is not null and trim(p.region_code) <> ''
    and p.region is not null and trim(p.region) <> ''
    and p.feature_id is not null
    and p.poi_class is not null
    and p.poi_type  is not null
),

cells as (
  select
    d.region_code::string as region_code,
    d.region::string as region,
    d.h3_r10::string      as h3_r10,
    d.cell_geog           as cell_geog,
    d.cell_area_m2::float as cell_area_m2,
    d.cell_wkt_4326::string        as cell_wkt_4326,
    d.cell_center_wkt_4326::string as cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }} d
  where d.region_code is not null and trim(d.region_code) <> ''
    and d.region is not null and trim(d.region) <> ''
    and d.h3_r10 is not null
    and d.cell_geog is not null
    and d.cell_area_m2 is not null
    and d.cell_area_m2 > 0
),

joined as (
  select
    c.region_code,
    c.region,
    c.h3_r10,
    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,
    b.feature_id,
    b.poi_class,
    b.poi_type,
    b.poi_geog,
    c.cell_geog,
    b.load_ts
  from base b
  join cells c
    on c.region_code = b.region_code
   and c.region = b.region
   and c.h3_r10      = b.h3_r10
  where st_intersects(b.poi_geog, c.cell_geog)
),

x as (
  select
    region_code,
    region,
    h3_r10,
    feature_id,
    poi_class,
    poi_type,
    st_area(st_intersection(poi_geog, cell_geog))::float as poi_xarea_m2,
    load_ts,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from joined
  where poi_geog is not null and cell_geog is not null
),

agg as (
  select
    region_code,
    region,
    h3_r10,

    any_value(cell_area_m2)         as cell_area_m2,
    any_value(cell_wkt_4326)        as cell_wkt_4326,
    any_value(cell_center_wkt_4326) as cell_center_wkt_4326,

    true as has_poi_areas,

    count(distinct feature_id) as poi_areas_cnt,
    sum(poi_xarea_m2) as poi_xarea_m2_sum,
    sum(poi_xarea_m2) / nullif(any_value(cell_area_m2), 0) as poi_xarea_share,

    -- 8 buckets: counts
    count(distinct iff(poi_class='amenity',  feature_id, null)) as poi_amenity_cnt,
    count(distinct iff(poi_class='shop',     feature_id, null)) as poi_shop_cnt,
    count(distinct iff(poi_class='tourism',  feature_id, null)) as poi_tourism_cnt,
    count(distinct iff(poi_class='office',   feature_id, null)) as poi_office_cnt,
    count(distinct iff(poi_class='leisure',  feature_id, null)) as poi_leisure_cnt,
    count(distinct iff(poi_class='sport',    feature_id, null)) as poi_sport_cnt,
    count(distinct iff(poi_class='building', feature_id, null)) as poi_building_cnt,
    count(distinct iff(poi_class='landuse',  feature_id, null)) as poi_landuse_cnt,

    -- 8 buckets: xarea sums
    sum(iff(poi_class='amenity',  poi_xarea_m2, 0)) as poi_amenity_xarea_m2_sum,
    sum(iff(poi_class='shop',     poi_xarea_m2, 0)) as poi_shop_xarea_m2_sum,
    sum(iff(poi_class='tourism',  poi_xarea_m2, 0)) as poi_tourism_xarea_m2_sum,
    sum(iff(poi_class='office',   poi_xarea_m2, 0)) as poi_office_xarea_m2_sum,
    sum(iff(poi_class='leisure',  poi_xarea_m2, 0)) as poi_leisure_xarea_m2_sum,
    sum(iff(poi_class='sport',    poi_xarea_m2, 0)) as poi_sport_xarea_m2_sum,
    sum(iff(poi_class='building', poi_xarea_m2, 0)) as poi_building_xarea_m2_sum,
    sum(iff(poi_class='landuse',  poi_xarea_m2, 0)) as poi_landuse_xarea_m2_sum,

    -- densities
    count(distinct feature_id) / nullif(any_value(cell_area_m2)/1e6, 0) as poi_areas_per_km2,
    sum(poi_xarea_m2)           / nullif(any_value(cell_area_m2)/1e6, 0) as poi_xarea_m2_per_km2,

    max(load_ts) as last_load_ts
  from x
  where poi_xarea_m2 is not null and poi_xarea_m2 > 0
  group by 1,2,3
)

select * from agg