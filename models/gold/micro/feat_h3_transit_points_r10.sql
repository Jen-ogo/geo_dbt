{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with pts as (
  select
    region_code::string as region_code,
    {{ h3_r10_from_geog_point('geog') }}::string as h3_r10,
    poi_class::string as poi_class,
    poi_type::string as poi_type,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('transit_points') }}
  where geog is not null
),

agg as (
  select
    region_code,
    h3_r10,
    count(*)::number(18,0) as transit_points_cnt,
    sum(iff(poi_class='transport', 1, 0))::number(18,0) as transport_points_cnt,
    sum(iff(poi_class='amenity',   1, 0))::number(18,0) as amenity_points_cnt,
    sum(iff(poi_class='emergency', 1, 0))::number(18,0) as emergency_points_cnt,
    sum(iff(lower(poi_type) in ('station','halt','tram_stop','subway_entrance'), 1, 0))::number(18,0) as station_like_cnt,
    sum(iff(lower(poi_type) in ('bus_stop','platform'), 1, 0))::number(18,0) as stop_like_cnt,
    max(load_ts) as last_load_ts
  from pts
  where h3_r10 is not null
  group by 1,2
)

select
  d.region_code,
  d.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,
  coalesce(a.transit_points_cnt, 0) as transit_points_cnt,
  coalesce(a.transport_points_cnt, 0) as transport_points_cnt,
  coalesce(a.amenity_points_cnt, 0) as amenity_points_cnt,
  coalesce(a.emergency_points_cnt, 0) as emergency_points_cnt,
  coalesce(a.station_like_cnt, 0) as station_like_cnt,
  coalesce(a.stop_like_cnt, 0) as stop_like_cnt,
  iff(coalesce(a.transit_points_cnt,0)=0, null, a.transport_points_cnt / nullif(a.transit_points_cnt,0))::float as transport_points_share,
  iff(coalesce(a.transit_points_cnt,0)=0, null, a.emergency_points_cnt / nullif(a.transit_points_cnt,0))::float as emergency_points_share,
  (coalesce(a.transit_points_cnt,0)  * 1e6 / nullif(d.cell_area_m2,0))::float as transit_points_per_km2,
  (coalesce(a.transport_points_cnt,0)* 1e6 / nullif(d.cell_area_m2,0))::float as transport_points_per_km2,
  a.last_load_ts,
  iff(coalesce(a.transit_points_cnt, 0) > 0, true, false) as has_transit_points
from {{ ref('dim_h3_r10_cells') }} d
left join agg a
  on a.region_code = d.region_code
 and a.h3_r10      = d.h3_r10