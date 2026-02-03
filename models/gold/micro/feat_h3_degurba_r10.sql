{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with cells as (
  select
    region_code::string as region_code,
    region::string as region,
    h3_r10::string      as h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,
    /* prefer canonical center geog if present in dim */
    h3_cell_to_point(h3_r10) as cell_center_geog
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
      and region is not null
    and h3_r10 is not null
    and cell_center_wkt_4326 is not null
),

lau as (
  select
    region_code::string as region_code,
    region::string as region,
    lau_id::string      as lau_id,
    lau_name::string    as lau_name,
    try_to_number(degurba)::int as degurba,
    try_to_number(year)::int   as year,
    geog as lau_geog,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('lau_degurba') }}
  where region_code is not null
  and region is not null
    and geog is not null
),

matched as (
  select
    c.region_code,
    c.region,
    c.h3_r10,
    l.year,
    l.degurba,
    l.lau_id,
    l.lau_name,
    l.load_ts as last_load_ts,
    row_number() over (
      partition by c.region_code, c.region, c.h3_r10
      order by l.year desc nulls last, l.load_ts desc nulls last, l.lau_id
    ) as rn
  from cells c
  join lau l
    on l.region_code = c.region_code
    and l.region = c.region
   and st_contains(l.lau_geog, c.cell_center_geog)
),

best as (
  select
    region_code, region, h3_r10, year, degurba, lau_id, lau_name, last_load_ts
  from matched
  where rn = 1
)

select
  c.region_code,
  c.region,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  b.year,
  b.lau_id,
  b.lau_name,
  b.degurba,

  iff(b.degurba = 1, 1, 0) as degurba_1_city,
  iff(b.degurba = 2, 1, 0) as degurba_2_towns_suburbs,
  iff(b.degurba = 3, 1, 0) as degurba_3_rural,

  b.last_load_ts
from cells c
left join best b
  on b.region_code = c.region_code
   and b.region = c.region
 and b.h3_r10      = c.h3_r10