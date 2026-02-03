{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with ap as (
  select
    region_code::string as region_code,
    region::string as region,
    feature_id::string  as feature_id,
    lower(coalesce(building_type::string, activity_type_lc::string)) as building_type_lc,
    greatest(coalesce(building_levels::int, 1), 1) as building_levels,
    geog,
    centroid_geog,
    geom_wkt_4326::string as geom_wkt_4326,
    load_ts::timestamp_ntz as load_ts
  from {{ ref('activity_places') }}
  where activity_class = 'building'
    and geog is not null
    and centroid_geog is not null
    and coalesce(building_type, activity_type_lc) is not null
    and lower(coalesce(building_type, activity_type_lc)) <> 'no'
    and (
      geom_wkt_4326 ilike 'POLYGON(%'
      or geom_wkt_4326 ilike 'MULTIPOLYGON(%'
    )
),

ap_model as (
  select *
  from ap
  where building_type_lc not in (
    'yes',
    'outbuilding','farm_auxiliary','shed','barn','sty','stable',
    'garage','garages','roof','greenhouse',
    'allotment_house',
    'hut','cabin'
  )
),

enriched as (
  select
    region_code,
    region,
    {{ h3_r10_from_geog_point('centroid_geog') }} as h3_r10,
    building_type_lc,
    building_levels,
    st_area(geog) as footprint_area_m2,
    load_ts
  from ap_model
),

classified as (
  select
    region_code,
    region,
    h3_r10,
    building_levels,
    footprint_area_m2,
    case
      when building_type_lc in (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) then 'residential'
      when building_type_lc in (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) then 'nonresidential'
      when building_type_lc = 'yes' then 'unknown'
      else 'other'
    end as building_group,
    load_ts
  from enriched
  where h3_r10 is not null
    and footprint_area_m2 is not null
    and footprint_area_m2 > 0
),

agg as (
  select
    region_code,
    region,
    h3_r10,

    count(*) as buildings_cnt,
    count_if(building_group = 'residential')    as res_buildings_cnt,
    count_if(building_group = 'nonresidential') as nonres_buildings_cnt,
    count_if(building_group = 'unknown')        as unknown_buildings_cnt,

    sum(footprint_area_m2) as footprint_area_m2_sum,
    sum(footprint_area_m2 * building_levels) as floor_area_m2_est_sum,

    avg(building_levels)::float as levels_avg,
    approx_percentile(building_levels, 0.5) as levels_p50,
    approx_percentile(footprint_area_m2, 0.5) as footprint_area_p50_m2,
    approx_percentile(footprint_area_m2, 0.9) as footprint_area_p90_m2,

    max(load_ts) as last_load_ts
  from classified
  group by 1,2,3
),

cell as (
  select
    region_code,
    region,
    h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
)

select
  c.region_code,
  c.region,
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

  a.buildings_cnt / nullif(c.cell_area_m2 / 1e6, 0) as buildings_per_km2,
  a.footprint_area_m2_sum / nullif(c.cell_area_m2 / 1e6, 0) as footprint_m2_per_km2,
  a.floor_area_m2_est_sum / nullif(c.cell_area_m2 / 1e6, 0) as floor_area_m2_per_km2,
  a.footprint_area_m2_sum / nullif(c.cell_area_m2, 0) as built_up_share,

  'activity_places_polygon'::string as building_method,
  a.last_load_ts
from agg a
join cell c
  on c.region_code = a.region_code
 and c.region = a.region
 and c.h3_r10      = a.h3_r10