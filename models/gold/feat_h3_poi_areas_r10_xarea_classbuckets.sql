{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with base as (
  select
    p.region_code,
    {{ h3_r10_from_geog_centroid('p.geog') }} as h3_r10,
    p.poi_class,
    p.load_ts,
    p.geog as poi_geog
  from {{ ref('poi_areas') }} p
  where p.geog is not null
    and p.poi_class is not null
),
x as (
  select
    b.region_code,
    b.h3_r10,
    b.poi_class,
    st_area(st_intersection(b.poi_geog, d.cell_geog)) as poi_xarea_m2,
    b.load_ts
  from base b
  join {{ ref('dim_h3_r10_cells') }} d
    on d.region_code = b.region_code
   and d.h3_r10      = b.h3_r10
  where st_intersects(b.poi_geog, d.cell_geog)
),
agg as (
  select
    region_code,
    h3_r10,
    count(*) as poi_areas_cnt,
    sum(poi_xarea_m2) as poi_xarea_m2_sum,
    sum(iff(poi_class='amenity', 1, 0)) as poi_amenity_cnt,
    sum(iff(poi_class='shop',    1, 0)) as poi_shop_cnt,
    sum(iff(poi_class='tourism', 1, 0)) as poi_tourism_cnt,
    sum(iff(poi_class='office',  1, 0)) as poi_office_cnt,
    sum(iff(poi_class='leisure', 1, 0)) as poi_leisure_cnt,
    sum(iff(poi_class='sport',   1, 0)) as poi_sport_cnt,
    sum(iff(poi_class='building',1, 0)) as poi_building_cnt,
    sum(iff(poi_class='landuse', 1, 0)) as poi_landuse_cnt,
    sum(iff(poi_class='amenity', poi_xarea_m2, 0)) as poi_amenity_xarea_m2_sum,
    sum(iff(poi_class='shop',    poi_xarea_m2, 0)) as poi_shop_xarea_m2_sum,
    sum(iff(poi_class='tourism', poi_xarea_m2, 0)) as poi_tourism_xarea_m2_sum,
    sum(iff(poi_class='office',  poi_xarea_m2, 0)) as poi_office_xarea_m2_sum,
    sum(iff(poi_class='leisure', poi_xarea_m2, 0)) as poi_leisure_xarea_m2_sum,
    sum(iff(poi_class='sport',   poi_xarea_m2, 0)) as poi_sport_xarea_m2_sum,
    sum(iff(poi_class='building',poi_xarea_m2, 0)) as poi_building_xarea_m2_sum,
    sum(iff(poi_class='landuse', poi_xarea_m2, 0)) as poi_landuse_xarea_m2_sum,
    max(load_ts) as last_load_ts
  from x
  group by 1,2
)

select
  a.region_code,
  a.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,
  true as has_poi_areas,
  a.poi_areas_cnt,
  a.poi_xarea_m2_sum,
  a.poi_xarea_m2_sum / nullif(d.cell_area_m2, 0) as poi_xarea_share,
  a.poi_areas_cnt / nullif(d.cell_area_m2 / 1e6, 0) as poi_areas_per_km2,
  a.poi_xarea_m2_sum / nullif(d.cell_area_m2 / 1e6, 0) as poi_xarea_m2_per_km2,
  a.poi_amenity_cnt, a.poi_shop_cnt, a.poi_tourism_cnt, a.poi_office_cnt,
  a.poi_leisure_cnt, a.poi_sport_cnt, a.poi_building_cnt, a.poi_landuse_cnt,
  a.poi_amenity_cnt / nullif(a.poi_areas_cnt, 0)  as poi_amenity_share,
  a.poi_shop_cnt    / nullif(a.poi_areas_cnt, 0)  as poi_shop_share,
  a.poi_tourism_cnt / nullif(a.poi_areas_cnt, 0)  as poi_tourism_share,
  a.poi_office_cnt  / nullif(a.poi_areas_cnt, 0)  as poi_office_share,
  a.poi_leisure_cnt / nullif(a.poi_areas_cnt, 0)  as poi_leisure_share,
  a.poi_sport_cnt   / nullif(a.poi_areas_cnt, 0)  as poi_sport_share,
  a.poi_building_cnt/ nullif(a.poi_areas_cnt, 0)  as poi_building_share,
  a.poi_landuse_cnt / nullif(a.poi_areas_cnt, 0)  as poi_landuse_share,
  a.poi_amenity_xarea_m2_sum, a.poi_shop_xarea_m2_sum, a.poi_tourism_xarea_m2_sum, a.poi_office_xarea_m2_sum,
  a.poi_leisure_xarea_m2_sum, a.poi_sport_xarea_m2_sum, a.poi_building_xarea_m2_sum, a.poi_landuse_xarea_m2_sum,
  a.poi_amenity_xarea_m2_sum / nullif(d.cell_area_m2, 0)  as poi_amenity_xarea_share,
  a.poi_shop_xarea_m2_sum    / nullif(d.cell_area_m2, 0)  as poi_shop_xarea_share,
  a.poi_tourism_xarea_m2_sum / nullif(d.cell_area_m2, 0)  as poi_tourism_xarea_share,
  a.poi_office_xarea_m2_sum  / nullif(d.cell_area_m2, 0)  as poi_office_xarea_share,
  a.poi_leisure_xarea_m2_sum / nullif(d.cell_area_m2, 0)  as poi_leisure_xarea_share,
  a.poi_sport_xarea_m2_sum   / nullif(d.cell_area_m2, 0)  as poi_sport_xarea_share,
  a.poi_building_xarea_m2_sum/ nullif(d.cell_area_m2, 0)  as poi_building_xarea_share,
  a.poi_landuse_xarea_m2_sum / nullif(d.cell_area_m2, 0)  as poi_landuse_xarea_share,
  a.last_load_ts
from agg a
join {{ ref('dim_h3_r10_cells') }} d
  on d.region_code = a.region_code
 and d.h3_r10      = a.h3_r10
;