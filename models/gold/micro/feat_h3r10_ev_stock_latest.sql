{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with admin4 as (
  select
    region_code::string as region_code,
    region::string      as region,
    osm_id::string      as admin_osm_id,
    name::string        as admin_name,
    geog                as admin_geog
  from {{ ref('admin_areas') }}
  where admin_level = 4
    and geog is not null
    and boundary = 'administrative'
),

nuts2 as (
  select
    cntr_code::string as cntr_code,
    nuts_id::string   as nuts_id,
    try_to_number(level)::int as nuts_level,
    name_latn::string as nuts_name,
    geog              as nuts_geog
  from {{ ref('gisco_nuts') }}
  where year  = '2024'
    and scale = '01m'
    and crs   = '4326'
    and try_to_number(level)::int = 2
    and geog is not null
),

/* project scope: NUTS2 whose centroid is within admin4 polygon for each region_code/region */
project_nuts2_scope as (
  select distinct
     a.region_code,
    a.region,
    a.admin_osm_id,
    a.admin_name,
    n.cntr_code,
    n.nuts_id,
    n.nuts_level,
    n.nuts_name
  from admin4 a
  join nuts2 n
    on st_within(st_centroid(n.nuts_geog), a.admin_geog)
),

cells as (
  select
    region_code::string as region_code,
    region::string as region,
    h3_r10::string      as h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,
    h3_cell_to_point(h3_r10) as cell_center_geog
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and region is not null
    and h3_r10 is not null
    and cell_center_wkt_4326 is not null
),

/* map each H3 cell center to a NUTS2 polygon in the project scope */
h3_to_nuts2 as (
  select
    c.region_code,
    s.region,
    c.h3_r10,

    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,

    s.cntr_code,
    s.nuts_id,
    s.nuts_level,
    s.nuts_name,

    n.nuts_geog,
    {{ geog_to_wkt('n.nuts_geog') }} as nuts_wkt_4326
  from cells c
  join project_nuts2_scope s
    on s.region_code = c.region_code
    and s.region = c.region
  join nuts2 n
    on n.cntr_code = s.cntr_code
   and n.nuts_id   = s.nuts_id
  where st_within(c.cell_center_geog, n.nuts_geog)
),

/* eurostat EV stock facts */
eu as (
  select
    geo::string as geo,
    year::int as year,
    vehicle::string as vehicle,
    unit::string    as unit,
    freq::string    as freq,
    value as value
  from {{ ref('eurostat_tran_r_elvehst') }}
  where freq='A'
    and vehicle in ('CAR','VG_LE3P5','BUS_MCO_TRO')
    and unit in ('NR','PC')
),


latest_year as (
  select geo, max(year) as car_year_latest
  from eu
  group by 1
),

eu_latest as (
  select e.geo, e.year, e.vehicle, e.unit, e.value
  from eu e
  join latest_year y
    on y.geo = e.geo
   and y.car_year_latest = e.year
),

pvt as (
  select
    geo,
    max(case when vehicle='CAR'         and unit='NR' then value end)::number(38,10) as car_ev_nr_latest,
    max(case when vehicle='CAR'         and unit='PC' then value end)::number(38,10) as car_ev_pc_latest,
    max(case when vehicle='VG_LE3P5'    and unit='NR' then value end)::number(38,10) as vg_le3p5_ev_nr_latest,
    max(case when vehicle='VG_LE3P5'    and unit='PC' then value end)::number(38,10) as vg_le3p5_ev_pc_latest,
    max(case when vehicle='BUS_MCO_TRO' and unit='NR' then value end)::number(38,10) as bus_ev_nr_latest,
    max(case when vehicle='BUS_MCO_TRO' and unit='PC' then value end)::number(38,10) as bus_ev_pc_latest
  from eu_latest
  group by 1
)

select
  m.region_code,
  m.region,
  m.h3_r10,

  m.cell_area_m2,
  m.cell_wkt_4326,
  m.cell_center_wkt_4326,

  m.cntr_code,
  m.nuts_id,
  m.nuts_level,
  m.nuts_name,
  m.nuts_geog,
  m.nuts_wkt_4326,

  y.car_year_latest,
  p.car_ev_nr_latest,
  p.car_ev_pc_latest,
  p.vg_le3p5_ev_nr_latest,
  p.vg_le3p5_ev_pc_latest,
  p.bus_ev_nr_latest,
  p.bus_ev_pc_latest

from h3_to_nuts2 m
join latest_year y
  on y.geo = m.nuts_id
join pvt p
  on p.geo = m.nuts_id

where y.car_year_latest is not null
  and p.car_ev_nr_latest is not null
  and p.car_ev_pc_latest is not null
  and p.vg_le3p5_ev_nr_latest is not null
  and p.vg_le3p5_ev_pc_latest is not null
  and p.bus_ev_nr_latest is not null
  and p.bus_ev_pc_latest is not null