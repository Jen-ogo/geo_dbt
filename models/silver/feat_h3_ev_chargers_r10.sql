{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with src as (
  select
    region_code,
    h3_point_to_cell_string({{ wkt_to_geog('geom_wkt_4326') }}, 10) as h3_r10,
    total_sockets_cnt::int as total_sockets_cnt,
    has_dc::boolean as has_dc,
    has_ac::boolean as has_ac,
    load_ts
  from {{ ref('ev_chargers') }}
  where geom_wkt_4326 is not null
),

agg as (
  select
    region_code,
    h3_r10,
    count(*) as chargers_cnt,
    sum(coalesce(total_sockets_cnt,0)) as sockets_cnt_sum,
    count_if(has_dc) as chargers_dc_cnt,
    count_if(has_ac) as chargers_ac_cnt,
    max(load_ts) as last_load_ts
  from src
  where h3_r10 is not null
  group by 1,2
)

select
  c.region_code,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.chargers_cnt,
  a.sockets_cnt_sum,
  a.chargers_dc_cnt,
  a.chargers_ac_cnt,
  a.last_load_ts
from {{ ref('dim_h3_r10_cells') }} c
left join agg a
  on a.region_code=c.region_code and a.h3_r10=c.h3_r10