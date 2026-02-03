{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','region','h3_r10'],
    tags=['gold']
) }}

with pop as (
  select
    region_code::string as region_code,
    region::string      as region,
    h3_r10::string      as h3_r10,

    cell_area_m2::float as cell_area_m2,
    cell_wkt_4326::string as cell_wkt_4326,
    cell_center_wkt_4326::string as cell_center_wkt_4326,

    pop_total::float as pop_total,
    last_load_ts::timestamp_ntz as pop_last_load_ts
  from {{ ref('feat_h3_pop_r10') }}
  where region_code is not null
    and h3_r10      is not null
),

ch_src as (
  select
    region_code::string as region_code,
    region::string      as region,

    /* считаем h3_r10 прямо тут (как в Databricks) */
    {{ h3_r10_from_geog_point("coalesce(geog, try_to_geography(geom_wkt_4326))") }}::string as h3_r10,

    total_sockets_cnt::number as total_sockets_cnt,
    iff(has_dc, true, false) as has_dc,
    iff(has_ac, true, false) as has_ac,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('ev_chargers') }}
  where region_code is not null
    and coalesce(geog, try_to_geography(geom_wkt_4326)) is not null
),

ch_agg as (
  select
    region_code,
    region,
    h3_r10,

    count(*) as chargers_cnt,
    sum(coalesce(total_sockets_cnt, 0)) as sockets_cnt_sum,
    sum(case when has_dc then 1 else 0 end) as chargers_dc_cnt,
    sum(case when has_ac then 1 else 0 end) as chargers_ac_cnt,

    max(load_ts) as chargers_last_load_ts
  from ch_src
  where h3_r10 is not null
  group by 1,2,3
)

select
  p.region_code,
  p.region,
  p.h3_r10,

  p.cell_area_m2,
  p.cell_wkt_4326,
  p.cell_center_wkt_4326,

  /* chargers (0 for empty cells) */
  coalesce(c.chargers_cnt, 0) as chargers_cnt,
  coalesce(c.sockets_cnt_sum, 0) as sockets_cnt_sum,
  coalesce(c.chargers_dc_cnt, 0) as chargers_dc_cnt,
  coalesce(c.chargers_ac_cnt, 0) as chargers_ac_cnt,

  /* population */
  p.pop_total,

  /* derived */
  case when p.pop_total > 0
    then coalesce(c.chargers_cnt, 0) * 10000.0 / p.pop_total
  end as chargers_per_10k_pop,

  case when p.pop_total > 0
    then coalesce(c.sockets_cnt_sum, 0) * 10000.0 / p.pop_total
  end as sockets_per_10k_pop,

  coalesce(c.chargers_cnt, 0) / nullif(p.cell_area_m2 / 1e6, 0.0) as chargers_per_km2,
  coalesce(c.sockets_cnt_sum, 0) / nullif(p.cell_area_m2 / 1e6, 0.0) as sockets_per_km2,

  /* recency */
  greatest(coalesce(c.chargers_last_load_ts, p.pop_last_load_ts), p.pop_last_load_ts) as last_load_ts

from pop p
left join ch_agg c
  on c.region_code = p.region_code
  and c.region = p.region
 and c.h3_r10      = p.h3_r10