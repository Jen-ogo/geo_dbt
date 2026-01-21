{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with grid as (
  -- 1km census grid cells already attributed to admin4 + has H3 (from centroid) but
  -- we DO NOT use that centroid H3; we will polyfill the whole cell polygon into H3 R10.
  select
    grid_id::string           as grid_id,
    admin4_region_code::string as region_code,
    admin4_osm_id::string     as admin4_osm_id,
    admin4_name::string       as admin4_name,

    pop_total::number         as pop_total,
    pop_male::number          as pop_male,
    pop_female::number        as pop_female,
    pop_age_lt15::number      as pop_age_lt15,
    pop_age_1564::number      as pop_age_1564,
    pop_age_ge65::number      as pop_age_ge65,
    emp_total::number         as emp_total,

    cell_geog                 as cell_geog,
    st_area(cell_geog)        as cell_area_m2,

    source_file::string       as source_file,
    load_ts::timestamp_ntz    as load_ts
  from {{ ref('census_grid_2021_admin4') }}
  where cell_geog is not null
    and region_code is not null
),

h3_candidates as (
  -- IMPORTANT:
  -- Snowflake H3_POLYFILL is expected to return an array (flatten it).
  -- If your account returns H3 indexes as INT, just adjust casts in one place below.
  select
    g.*,
    f.value::string as h3_r10
  from grid g,
       lateral flatten(input => h3_polyfill(g.cell_geog, 10)) f
  where f.value is not null
),

h3_weighted as (
  -- allocate grid metrics to each H3 by intersection area share
  select
    region_code,
    admin4_osm_id,
    admin4_name,
    grid_id,
    h3_r10,

    cell_geog,
    cell_area_m2,

    h3_cell_to_boundary(h3_r10) as h3_cell_geog,
    st_area(
      st_intersection(cell_geog, h3_cell_to_boundary(h3_r10))
    ) as inter_area_m2,

    pop_total,
    pop_male,
    pop_female,
    pop_age_lt15,
    pop_age_1564,
    pop_age_ge65,
    emp_total,

    source_file,
    load_ts
  from h3_candidates
),

h3_alloc as (
  select
    region_code,
    admin4_osm_id,
    admin4_name,
    grid_id,
    h3_r10,

    inter_area_m2,
    cell_area_m2,

    iff(cell_area_m2 > 0, inter_area_m2 / cell_area_m2, null) as w,

    pop_total,
    pop_male,
    pop_female,
    pop_age_lt15,
    pop_age_1564,
    pop_age_ge65,
    emp_total,

    source_file,
    load_ts
  from h3_weighted
  where inter_area_m2 is not null
    and inter_area_m2 > 0
    and cell_area_m2 is not null
    and cell_area_m2 > 0
),

agg as (
  select
    region_code,
    h3_r10,
    admin4_osm_id,
    admin4_name,

    sum(coalesce(pop_total,0)    * coalesce(w,0)) as pop_total,
    sum(coalesce(pop_male,0)     * coalesce(w,0)) as pop_male,
    sum(coalesce(pop_female,0)   * coalesce(w,0)) as pop_female,
    sum(coalesce(pop_age_lt15,0) * coalesce(w,0)) as pop_age_lt15,
    sum(coalesce(pop_age_1564,0) * coalesce(w,0)) as pop_age_1564,
    sum(coalesce(pop_age_ge65,0) * coalesce(w,0)) as pop_age_ge65,
    sum(coalesce(emp_total,0)    * coalesce(w,0)) as emp_total,

    iff(sum(coalesce(pop_total,0) * coalesce(w,0)) > 0,
        sum(coalesce(pop_age_ge65,0) * coalesce(w,0)) / sum(coalesce(pop_total,0) * coalesce(w,0)),
        null
    ) as share_age_ge65,

    iff(sum(coalesce(pop_total,0) * coalesce(w,0)) > 0,
        sum(coalesce(pop_age_lt15,0) * coalesce(w,0)) / sum(coalesce(pop_total,0) * coalesce(w,0)),
        null
    ) as share_age_lt15,

    iff(sum(coalesce(pop_total,0) * coalesce(w,0)) > 0,
        sum(coalesce(emp_total,0) * coalesce(w,0)) / sum(coalesce(pop_total,0) * coalesce(w,0)),
        null
    ) as share_emp,

    count(distinct grid_id) as grid_cells_cnt,
    max(load_ts) as last_load_ts
  from h3_alloc
  where h3_r10 is not null
  group by 1,2,3,4
),

cell as (
  select distinct
    region_code,
    h3_r10,
    h3_cell_to_boundary(h3_r10)         as cell_geog,
    st_aswkt(h3_cell_to_boundary(h3_r10)) as cell_wkt_4326,
    st_area(h3_cell_to_boundary(h3_r10))  as cell_area_m2,
    h3_cell_to_point(h3_r10)            as cell_center_geog,
    st_aswkt(h3_cell_to_point(h3_r10))   as cell_center_wkt_4326
  from agg
)

select
  a.region_code,
  a.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.admin4_osm_id,
  a.admin4_name,

  a.pop_total,
  a.pop_male,
  a.pop_female,
  a.pop_age_lt15,
  a.pop_age_1564,
  a.pop_age_ge65,
  a.emp_total,

  a.share_age_ge65,
  a.share_age_lt15,
  a.share_emp,

  a.grid_cells_cnt,
  a.last_load_ts
from agg a
join cell c
  on c.region_code = a.region_code
 and c.h3_r10 = a.h3_r10
;