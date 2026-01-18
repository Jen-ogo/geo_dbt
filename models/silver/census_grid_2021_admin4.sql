{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH'
) }}

with admin4 as (
  select
    osm_id::string as admin4_osm_id,
    name::string   as admin4_name,
    region_code::string as admin4_region_code,
    {{ wkt_to_geog('geom_wkt_4326') }} as admin4_geog
  from {{ ref('admin_areas') }}
  where admin_level = 4
    and boundary = 'administrative'
    and geom_wkt_4326 is not null
),

grid_src as (
  select
    grd_id::string as grid_id,

    {{ nullif_neg9999('t') }}       as pop_total,
    {{ nullif_neg9999('m') }}       as pop_male,
    {{ nullif_neg9999('f') }}       as pop_female,
    {{ nullif_neg9999('y_lt15') }}  as pop_age_lt15,
    {{ nullif_neg9999('y_1564') }}  as pop_age_1564,
    {{ nullif_neg9999('y_ge65') }}  as pop_age_ge65,
    {{ nullif_neg9999('emp') }}     as emp_total,

    land_surface::float as land_surface,
    {{ nullif_neg9999('populated') }} as populated,

    {{ wkt_to_geog('geom_wkt') }} as cell_geog,

    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts
  from {{ source('bronze','CENSUS_GRID_2021_EUROPE') }}
  where geom_wkt is not null
    and {{ nullif_neg9999('t') }} is not null
),

grid_geo as (
  select
    *,
    st_centroid(cell_geog)          as cell_pt,
    {{ geog_to_wkt('cell_geog') }}  as cell_wkt_4326
  from grid_src
  where cell_geog is not null
),

joined as (
  select
    g.*,
    a.admin4_osm_id,
    a.admin4_name,
    a.admin4_region_code,
    {{ h3_r10_from_geog_point('g.cell_pt') }} as h3_r10
  from grid_geo g
  join admin4 a
    on st_contains(a.admin4_geog, g.cell_pt)
),

final as (
  select *
  from joined
  {{ dedup_qualify(partition_by=['grid_id'], order_by=['load_ts desc','source_file desc']) }}
)

select * from final