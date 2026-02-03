{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r7']
) }}

with pts as (
  select
    region_code::string as region_code,
    region::string as region,
    h3_point_to_cell_string(geog, 7)::string as h3_r7,
    lower(poi_class::string) as poi_class,
    lower(poi_type::string)  as poi_type,
    load_ts::timestamp_ntz   as load_ts
  from {{ ref('transit_points') }}
  where region_code is not null
    and region is not null
    and geog is not null
),

agg as (
  select
    region_code,
    region,
    h3_r7,

    count(*)::number(18,0) as transit_points_cnt,
    sum(iff(poi_class='transport', 1, 0))::number(18,0) as transport_points_cnt,
    sum(iff(poi_class='amenity',   1, 0))::number(18,0) as amenity_points_cnt,
    sum(iff(poi_class='emergency', 1, 0))::number(18,0) as emergency_points_cnt,

    sum(iff(poi_type in ('station','halt','tram_stop','subway_entrance'), 1, 0))::number(18,0) as station_like_cnt,
    sum(iff(poi_type in ('bus_stop','platform'), 1, 0))::number(18,0) as stop_like_cnt,

    max(load_ts) as last_load_ts
  from pts
  where h3_r7 is not null
  group by 1,2,3
),

cells as (
  select
    region_code::string as region_code,
    region::string as region,
    h3_r7::string       as h3_r7,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }}
  where region_code is not null
    and region is not null
    and h3_r7 is not null
)

select
  c.region_code,
  c.region,
  c.h3_r7,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  coalesce(a.transit_points_cnt, 0)    as transit_points_cnt,
  coalesce(a.transport_points_cnt, 0)  as transport_points_cnt,
  coalesce(a.amenity_points_cnt, 0)    as amenity_points_cnt,
  coalesce(a.emergency_points_cnt, 0)  as emergency_points_cnt,
  coalesce(a.station_like_cnt, 0)      as station_like_cnt,
  coalesce(a.stop_like_cnt, 0)         as stop_like_cnt,

  iff(coalesce(a.transit_points_cnt,0)=0, null,
      a.transport_points_cnt / nullif(a.transit_points_cnt,0))::float as transport_points_share,

  iff(coalesce(a.transit_points_cnt,0)=0, null,
      a.emergency_points_cnt / nullif(a.transit_points_cnt,0))::float as emergency_points_share,

  (coalesce(a.transit_points_cnt,0)   * 1e6 / nullif(c.cell_area_m2,0))::float as transit_points_per_km2,
  (coalesce(a.transport_points_cnt,0) * 1e6 / nullif(c.cell_area_m2,0))::float as transport_points_per_km2,

  a.last_load_ts,
  (coalesce(a.transit_points_cnt, 0) > 0) as has_transit_points
from cells c
left join agg a
  on a.region_code = c.region_code
 and a.region = c.region
 and a.h3_r7      = c.h3_r7