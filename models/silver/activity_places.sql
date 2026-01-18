{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags,
    {{ wkt_to_geog('b.geom_wkt') }} as geog
  from {{ source('bronze','BUILDINGS_ACTIVITY') }} b
),

typed as (
  select
    coalesce(
      iff(osm_id is not null, 'N' || osm_id::string, null),
      iff(osm_way_id is not null, 'W' || osm_way_id::string, null)
    ) as feature_id,

    osm_id::string as osm_id,
    osm_way_id::string as osm_way_id,

    nullif(name,'')::string as name,
    tags:"name:en"::string  as name_en,

    case
      when coalesce(amenity::string, tags:"amenity"::string) is not null then 'amenity'
      when coalesce(shop::string, tags:"shop"::string) is not null then 'shop'
      when coalesce(office::string, tags:"office"::string) is not null then 'office'
      when coalesce(tourism::string, tags:"tourism"::string) is not null then 'tourism'
      when coalesce(leisure::string, tags:"leisure"::string) is not null then 'leisure'
      when coalesce(sport::string, tags:"sport"::string) is not null then 'sport'
      when coalesce(craft::string, tags:"craft"::string) is not null then 'craft'
      when coalesce(building::string, tags:"building"::string) is not null then 'building'
      else null
    end as activity_class,

    lower(coalesce(
      amenity::string, tags:"amenity"::string,
      shop::string, tags:"shop"::string,
      office::string, tags:"office"::string,
      tourism::string, tags:"tourism"::string,
      leisure::string, tags:"leisure"::string,
      sport::string, tags:"sport"::string,
      craft::string, tags:"craft"::string,
      building::string, tags:"building"::string
    )) as activity_type_lc,

    geog as geog,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326,

    region::string as region_code,
    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts
  from src
),

filtered as (
  select *
  from typed
  where feature_id is not null
    and geog is not null
    and activity_class is not null
    and activity_type_lc is not null
  {{ dedup_qualify(partition_by=['feature_id'], order_by=['load_ts desc','source_file desc']) }}
)

select * from filtered