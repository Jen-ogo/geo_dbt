{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with all_h3 as (

  -- EV chargers (points)
  select
    region_code,
    region,
    h3_point_to_cell_string({{ wkt_to_geog('geom_wkt_4326') }}, 7) as h3_r7
  from {{ ref('ev_chargers') }}
  where geom_wkt_4326 is not null

  union all
  -- road segments (lines) -> centroid
  select
    region_code,
    region,
    h3_point_to_cell_string(st_centroid({{ wkt_to_geog('geom_wkt_4326') }}), 7) as h3_r7
  from {{ ref('road_segments') }}
  where geom_wkt_4326 is not null

  union all
  -- POI points
  select
    region_code,
    region,
    h3_point_to_cell_string({{ wkt_to_geog('geom_wkt_4326') }}, 7) as h3_r7
  from {{ ref('poi_points') }}
  where geom_wkt_4326 is not null

  union all
  -- POI areas (polygons) -> centroid
  select
    region_code,
    region,
    h3_point_to_cell_string(st_centroid({{ wkt_to_geog('geom_wkt_4326') }}), 7) as h3_r7
  from {{ ref('poi_areas') }}
  where geom_wkt_4326 is not null

  union all
  -- transit points
  select
    region_code,
    region,
    h3_point_to_cell_string({{ wkt_to_geog('geom_wkt_4326') }}, 7) as h3_r7
  from {{ ref('transit_points') }}
  where geom_wkt_4326 is not null

  union all
  -- transit lines -> centroid
  select
    region_code,
    region,
    h3_point_to_cell_string(st_centroid({{ wkt_to_geog('geom_wkt_4326') }}), 7) as h3_r7
  from {{ ref('transit_lines') }}
  where geom_wkt_4326 is not null

  union all
  -- activity places (polygons) -> centroid
  select
    region_code,
    region,
    h3_point_to_cell_string(st_centroid({{ wkt_to_geog('geom_wkt_4326') }}), 7) as h3_r7
  from {{ ref('activity_places') }}
  where geom_wkt_4326 is not null
),

distinct_h3 as (
  select distinct
    region_code as region_code,
    region as region,
    h3_r7::string       as h3_r7
  from all_h3
  where region_code is not null
    and region is not null
    and h3_r7 is not null
)

select
  region_code,
  region,
  h3_r7,

  h3_cell_to_boundary(h3_r7)                      as cell_geog,
  {{ geog_to_wkt('h3_cell_to_boundary(h3_r7)') }} as cell_wkt_4326,
  st_area(h3_cell_to_boundary(h3_r7))             as cell_area_m2,

  h3_cell_to_point(h3_r7)                         as cell_center_geog,
  {{ geog_to_wkt('h3_cell_to_point(h3_r7)') }}    as cell_center_wkt_4326
from distinct_h3