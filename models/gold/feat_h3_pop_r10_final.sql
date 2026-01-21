{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with s as (
  select *
  from {{ ref('feat_h3_pop_r10') }}
)

select
  region_code,
  h3_r10,

  cell_area_m2,
  cell_wkt_4326,
  cell_center_wkt_4326,

  admin4_osm_id,
  admin4_name,

  pop_total,
  pop_male,
  pop_female,
  pop_age_lt15,
  pop_age_1564,
  pop_age_ge65,
  emp_total,

  share_age_ge65,
  share_age_lt15,
  share_emp,

  grid_cells_cnt,

  /* support: covered by grid */
  (grid_cells_cnt * 1000000)::number as support_area_m2,
  'census_grid_1km_to_h3_admin4'::string as pop_method,

  /* densities by GRID support (км²) */
  pop_total / nullif(grid_cells_cnt, 0) as pop_per_km2_support,
  emp_total / nullif(grid_cells_cnt, 0) as emp_per_km2_support,

  /* densities by HEX area (км²) */
  pop_total / nullif(cell_area_m2 / 1e6, 0) as pop_per_km2_hex,
  emp_total / nullif(cell_area_m2 / 1e6, 0) as emp_per_km2_hex

from s;