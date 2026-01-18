{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags,
    {{ wkt_to_geog('b.geom_wkt') }} as geog
  from {{ source('bronze','BUILDINGS_ACTIVITY') }} b
),

base as (
  select
    coalesce(
      case when osm_id is not null then 'N' || osm_id::string end,
      case when osm_way_id is not null then 'W' || osm_way_id::string end
    ) as feature_id,

    osm_id::string as osm_id,
    osm_way_id::string as osm_way_id,

    coalesce(building::string, tags:"building"::string) as building_type,
    {{ safe_num('tags:"building:levels"::string') }}::int as building_levels,

    geog as geog,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326,

    st_centroid(geog) as centroid_geog,
    {{ h3_r10_from_geog_point('st_centroid(geog)') }} as h3_r10,

    region::string as region_code,
    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts
  from src
),

filtered as (
  select *
  from base
  where feature_id is not null
    and geog is not null
    and building_type is not null
    and lower(building_type) <> 'no'
  {{ dedup_qualify(partition_by=['feature_id'], order_by=['load_ts desc','source_file desc']) }}
)

select * from filtered