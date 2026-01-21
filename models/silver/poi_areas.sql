{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH'
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','POI_POLYGONS') }} b
),

typed as (
  select
    coalesce(nullif(osm_id,''), 'W' || osm_way_id::string)::string as feature_id,
    nullif(osm_id,'')::string as osm_id,
    osm_way_id::string as osm_way_id,
    nullif(name,'')::string as name,
    case
      when amenity is not null then 'amenity'
      when shop is not null then 'shop'
      when tourism is not null then 'tourism'
      when office is not null then 'office'
      when leisure is not null then 'leisure'
      when sport is not null then 'sport'
      when building is not null then 'building'
      when landuse is not null then 'landuse'
      else null
    end::string as poi_class,
    coalesce(amenity, shop, tourism, office, leisure, sport, building, landuse)::string as poi_type,

    {{ wkt_to_geog('geom_wkt') }} as geog,
    region::string as region_code,
    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts,
    tags as tags,
    other_tags::string as other_tags_raw
  from src
),

geo as (
  select
    *,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326
  from typed
  where geog is not null
    and poi_class is not null
    and poi_type is not null
),

final as (
  select *
  from geo
  {{ dedup_qualify(
      partition_by=['feature_id'],
      order_by=['load_ts desc','source_file desc']
  ) }}
)

select * from final