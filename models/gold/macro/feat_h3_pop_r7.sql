{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r7']
) }}

with grid as (
  select
    grid_id::string             as grid_id,
    admin4_region_code::string  as region_code,
    admin4_osm_id::string       as admin4_osm_id,
    admin4_name::string         as admin4_name,

    pop_total::float            as pop_total,
    pop_male::float             as pop_male,
    pop_female::float           as pop_female,
    pop_age_lt15::float         as pop_age_lt15,
    pop_age_1564::float         as pop_age_1564,
    pop_age_ge65::float         as pop_age_ge65,
    emp_total::float            as emp_total,

    cell_geog                   as grid_geog,
    st_area(cell_geog)::float   as grid_area_m2,

    source_file::string         as source_file,
    load_ts::timestamp_ntz      as load_ts
  from {{ ref('census_grid_2021_admin4') }}
  where cell_geog is not null
    and admin4_region_code is not null
    and grid_id is not null
    and pop_total is not null
),

-- cover each 1km polygon into candidate H3 R7 cells (strings)
candidates as (
  select
    g.*,
    f.value::string as h3_r7
  from grid g,
       lateral flatten(input => h3_coverage_strings(g.grid_geog, 7)) f
  where f.value is not null
),

-- join to canonical H3 cell geometries (avoid recomputing boundaries)
cand_with_cells as (
  select
    c.region_code,
    d.region,
    c.admin4_osm_id,
    c.admin4_name,
    c.grid_id,
    c.h3_r7,

    c.pop_total,
    c.pop_male,
    c.pop_female,
    c.pop_age_lt15,
    c.pop_age_1564,
    c.pop_age_ge65,
    c.emp_total,

    c.grid_geog,
    c.grid_area_m2,

    d.cell_geog as h3_cell_geog,
    c.load_ts
  from candidates c
  join {{ ref('dim_h3_r7_cells') }} d
    on d.region_code = c.region_code
   and d.h3_r7      = c.h3_r7
  where d.cell_geog is not null
    and st_intersects(c.grid_geog, d.cell_geog)
),

weighted as (
  select
    region_code,
    region,
    admin4_osm_id,
    admin4_name,
    grid_id,
    h3_r7,

    grid_area_m2,
    st_area(st_intersection(grid_geog, h3_cell_geog))::float as inter_area_m2,

    pop_total,
    pop_male,
    pop_female,
    pop_age_lt15,
    pop_age_1564,
    pop_age_ge65,
    emp_total,

    load_ts
  from cand_with_cells
),

alloc as (
  select
    region_code,
    region,
    admin4_osm_id,
    admin4_name,
    grid_id,
    h3_r7,

    inter_area_m2,
    grid_area_m2,

    iff(grid_area_m2 > 0, inter_area_m2 / grid_area_m2, null) as w,

    pop_total,
    pop_male,
    pop_female,
    pop_age_lt15,
    pop_age_1564,
    pop_age_ge65,
    emp_total,

    load_ts
  from weighted
  where inter_area_m2 is not null
    and inter_area_m2 > 0
    and grid_area_m2 is not null
    and grid_area_m2 > 0
),

agg as (
  select
    region_code,
    region,
    h3_r7,
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
  from alloc
  group by 1,2,3,4,5
)

select
  a.region_code,
  a.region,
  a.h3_r7,

  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

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
  (a.grid_cells_cnt * 1000000)::number as support_area_m2,

  'census_grid_1km_coverage_xarea_admin4_r7'::string as pop_method,

  -- densities by GRID support (km²)
  a.pop_total / nullif(a.grid_cells_cnt, 0) as pop_per_km2_support,
  a.emp_total / nullif(a.grid_cells_cnt, 0) as emp_per_km2_support,

  -- densities by HEX area (km²)
  a.pop_total / nullif(d.cell_area_m2 / 1e6, 0) as pop_per_km2_hex,
  a.emp_total / nullif(d.cell_area_m2 / 1e6, 0) as emp_per_km2_hex,

  a.last_load_ts
from agg a
join {{ ref('dim_h3_r7_cells') }} d
  on d.region_code = a.region_code
 and d.region = a.region
 and d.h3_r7       = a.h3_r7