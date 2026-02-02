{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','region','activity_class']
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags,
    {{ wkt_to_geog('b.geom_wkt') }}     as geog
  from {{ source('bronze','BUILDINGS_ACTIVITY') }} b
  where b.geom_wkt is not null
),

base as (
  select
    /* stable feature id like in Databricks (N/W prefix) */
    coalesce(
      iff(osm_id is not null and trim(osm_id::string) <> '', 'N' || osm_id::string, null),
      iff(osm_way_id is not null and trim(osm_way_id::string) <> '', 'W' || osm_way_id::string, null)
    ) as feature_id,

    nullif(trim(osm_id::string), '')     as osm_id,
    nullif(trim(osm_way_id::string), '') as osm_way_id,

    nullif(name::string,'') as name,
    tags:"name:en"::string  as name_en,

    /* activity class (same decision tree) */
    case
      when coalesce(amenity::string, tags:"amenity"::string) is not null then 'amenity'
      when coalesce(shop::string,    tags:"shop"::string)    is not null then 'shop'
      when coalesce(office::string,  tags:"office"::string)  is not null then 'office'
      when coalesce(tourism::string, tags:"tourism"::string) is not null then 'tourism'
      when coalesce(leisure::string, tags:"leisure"::string) is not null then 'leisure'
      when coalesce(sport::string,   tags:"sport"::string)   is not null then 'sport'
      when coalesce(craft::string,   tags:"craft"::string)   is not null then 'craft'
      when coalesce(building::string, tags:"building"::string) is not null then 'building'
      else null
    end as activity_class,

    lower(coalesce(
      amenity::string,  tags:"amenity"::string,
      shop::string,     tags:"shop"::string,
      office::string,   tags:"office"::string,
      tourism::string,  tags:"tourism"::string,
      leisure::string,  tags:"leisure"::string,
      sport::string,    tags:"sport"::string,
      craft::string,    tags:"craft"::string,
      building::string, tags:"building"::string
    )) as activity_type_lc,

    /* building extras (we keep them, but no H3 here) */
    lower(coalesce(building::string, tags:"building"::string)) as building_type,

    -- FIX #1: clamp building_levels to >= 1 (prevents negative/zero values)
    greatest(
      1,
      coalesce({{ safe_num('tags:"building:levels"::string') }}::int, 1)
    ) as building_levels,

    coalesce(tags:"operator"::string, tags:"network"::string, tags:"brand"::string) as operator_name,
    tags:"opening_hours"::string as opening_hours,

    /* geometry */
    geog as geog,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326,

    case
      when geog is null then null
      when {{ geog_to_wkt('geog') }} ilike 'POINT%' then geog
      else st_centroid(geog)
    end as centroid_geog,

    /* lineage */
    lower(region::string) as region_code,
    nullif(trim(region::string),'') as region,

    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts,

    /* raw tags for debugging */
    tags as tags,
    other_tags::string as other_tags_raw

  from src
),

filtered as (
  select *
  from base
  where feature_id is not null
    and geog is not null
    and geom_wkt_4326 is not null
    and activity_class is not null
    and activity_type_lc is not null
    and region_code is not null and trim(region_code) <> ''
    and region is not null and trim(region) <> ''
),

dedup as (
  select *
  from filtered
  {{ dedup_qualify(
      partition_by=['region_code','region','feature_id'],
      order_by=['load_ts desc','source_file desc']
  ) }}
)

select * from dedup