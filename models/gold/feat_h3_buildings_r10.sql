{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with b as (
  select
    region_code::string               as region_code,
    h3_r10::string                    as h3_r10,
    lower(building_type::string)      as building_type,
    coalesce(building_levels::int, 1) as building_levels,
    geog                               as geog,
    load_ts::timestamp_ntz            as load_ts
  from {{ ref('building_footprints_model') }}
  where h3_r10 is not null
    and geog is not null
),

cell as (
  select distinct
    region_code,
    h3_r10,
    h3_cell_to_boundary(h3_r10)           as cell_geog,
    st_aswkt(h3_cell_to_boundary(h3_r10)) as cell_wkt_4326,
    st_area(h3_cell_to_boundary(h3_r10))  as cell_area_m2,
    h3_cell_to_point(h3_r10)              as cell_center_geog,
    st_aswkt(h3_cell_to_point(h3_r10))    as cell_center_wkt_4326
  from b
),

b2 as (
  select
    region_code,
    h3_r10,
    building_levels,
    st_area(geog) as footprint_area_m2,
    case
      when building_type in (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) then 'residential'

      when building_type in (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) then 'nonresidential'

      when building_type = 'yes' then 'unknown'
      else 'other'
    end as building_group,
    load_ts
  from b
),

agg as (
  select
    region_code,
    h3_r10,

    count(*)                                    as buildings_cnt,
    count_if(building_group = 'residential')    as res_buildings_cnt,
    count_if(building_group = 'nonresidential') as nonres_buildings_cnt,
    count_if(building_group = 'unknown')        as unknown_buildings_cnt,

    sum(footprint_area_m2)                         as footprint_area_m2_sum,
    sum(footprint_area_m2 * building_levels)       as floor_area_m2_est_sum,

    avg(building_levels)::float                    as levels_avg,
    approx_percentile(building_levels, 0.5)        as levels_p50,

    approx_percentile(footprint_area_m2, 0.5)      as footprint_area_p50_m2,
    approx_percentile(footprint_area_m2, 0.9)      as footprint_area_p90_m2,

    max(load_ts)                                   as last_load_ts
  from b2
  group by 1,2
)

select
  c.region_code,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.buildings_cnt,
  a.res_buildings_cnt,
  a.nonres_buildings_cnt,
  a.unknown_buildings_cnt,

  a.footprint_area_m2_sum,
  a.floor_area_m2_est_sum,

  a.levels_avg,
  a.levels_p50,
  a.footprint_area_p50_m2,
  a.footprint_area_p90_m2,

  /* densities per km2 */
  a.buildings_cnt / nullif(c.cell_area_m2 / 1e6, 0)         as buildings_per_km2,
  a.footprint_area_m2_sum / nullif(c.cell_area_m2 / 1e6, 0) as footprint_m2_per_km2,
  a.floor_area_m2_est_sum / nullif(c.cell_area_m2 / 1e6, 0) as floor_area_m2_per_km2,

  /* built-up share (0..1 in ideal world; may exceed if invalid/overlaps) */
  a.footprint_area_m2_sum / nullif(c.cell_area_m2, 0)       as built_up_share,

  a.last_load_ts
from cell c
join agg a
  on a.region_code = c.region_code
 and a.h3_r10       = c.h3_r10