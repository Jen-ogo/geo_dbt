{{ config(materialized='table') }}

with rowcnt as (
  select
    'feat_h3_buildings_r10' as model_name,
    count(*)::number as row_count
  from {{ ref('feat_h3_buildings_r10') }}
),

samples as (
  select
    region_code,
    h3_r10,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('feat_h3_buildings_r10') }}
  where cell_wkt_4326 is not null
  limit 10
)

select
  r.model_name,
  r.row_count,
  s.region_code,
  s.h3_r10,
  s.cell_wkt_4326,
  s.cell_center_wkt_4326
from rowcnt r
left join samples s
  on 1=1;