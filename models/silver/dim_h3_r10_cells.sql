{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with all_h3 as (

  select
    region_code,
    {{ h3_r10_from_geog_point( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('ev_chargers') }}
  where geom_wkt_4326 is not null

  union all
  select
    region_code,
    {{ h3_r10_from_geog_centroid( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('road_segments') }}
  where geom_wkt_4326 is not null

  union all
  select
    region_code,
    {{ h3_r10_from_geog_point( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('poi_points') }}
  where geom_wkt_4326 is not null

  union all
  select
    region_code,
    {{ h3_r10_from_geog_centroid( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('poi_areas') }}
  where geom_wkt_4326 is not null

  union all
  select
    region_code,
    {{ h3_r10_from_geog_point( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('transit_points') }}
  where geom_wkt_4326 is not null

  union all
  select
    region_code,
    {{ h3_r10_from_geog_centroid( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('transit_lines') }}
  where geom_wkt_4326 is not null

  union all
  select
    region_code,
    {{ h3_r10_from_geog_centroid( wkt_to_geog('geom_wkt_4326') ) }} as h3_r10
  from {{ ref('activity_places') }}
  where geom_wkt_4326 is not null
),

distinct_h3 as (
  select distinct
    region_code::string as region_code,
    h3_r10::string      as h3_r10
  from all_h3
  where h3_r10 is not null
)

select
  region_code,
  h3_r10,

  h3_cell_to_boundary(h3_r10)                        as cell_geog,
  {{ geog_to_wkt('h3_cell_to_boundary(h3_r10)') }}   as cell_wkt_4326,
  st_area(h3_cell_to_boundary(h3_r10))               as cell_area_m2,

  h3_cell_to_point(h3_r10)                           as cell_center_geog,
  {{ geog_to_wkt('h3_cell_to_point(h3_r10)') }}      as cell_center_wkt_4326
from distinct_h3