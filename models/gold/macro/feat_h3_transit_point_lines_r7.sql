{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r7']
) }}

with tp as (
  select
    region_code::string as region_code,
    region_code::string      as region,
    h3_point_to_cell_string(geog, 7)::string as h3_r7,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('transit_points') }}
  where geog is not null
    and region_code is not null
),

tl as (
  select
    region_code::string as region_code,
    region_code::string      as region,
    h3_point_to_cell_string(st_centroid(geog), 7)::string as h3_r7,
    /* Snowflake GEOGRAPHY length is in meters (geodesic) */
    st_length(geog)::float as len_m,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('transit_lines') }}
  where geog is not null
    and region_code is not null
),

agg_points as (
  select
    region_code,
    h3_r7,
    iff(count(distinct region)=1, max(region), null) as region,
    count(*)::number(18,0) as transit_points_cnt,
    max(load_ts) as last_points_load_ts
  from tp
  where h3_r7 is not null
  group by 1,2
),

agg_lines as (
  select
    region_code,
    h3_r7,
    iff(count(distinct region)=1, max(region), null) as region,
    sum(len_m)::float as transit_lines_len_m_sum,
    max(load_ts) as last_lines_load_ts
  from tl
  where h3_r7 is not null
  group by 1,2
),

cells as (
  select
    region_code::string as region_code,
    h3_r7::string       as h3_r7,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }}
  where region_code is not null
    and h3_r7 is not null
)

select
  c.region_code,

  case
    when p.region is null and l.region is null then null
    when p.region is not null and l.region is null then p.region
    when p.region is null and l.region is not null then l.region
    when p.region = l.region then p.region
    else null
  end as region,

  c.h3_r7,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  coalesce(p.transit_points_cnt, 0) as transit_points_cnt,
  coalesce(l.transit_lines_len_m_sum, 0) as transit_lines_len_m_sum,

  /* densities per km² */
  (coalesce(p.transit_points_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0))::float as transit_points_per_km2,
  (coalesce(l.transit_lines_len_m_sum, 0) * 1e6 / nullif(c.cell_area_m2, 0.0))::float as transit_lines_m_per_km2,

  (coalesce(p.transit_points_cnt, 0) > 0 or coalesce(l.transit_lines_len_m_sum, 0) > 0) as has_transit,

  case
    when p.last_points_load_ts is null and l.last_lines_load_ts is null then null
    else greatest(p.last_points_load_ts, l.last_lines_load_ts)
  end as last_load_ts

from cells c
left join agg_points p
  on p.region_code = c.region_code
 and p.h3_r7       = c.h3_r7
left join agg_lines l
  on l.region_code = c.region_code
 and l.h3_r7       = c.h3_r7