{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH'
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','POI_POINTS') }} b
  where b.osm_id is not null
),

typed as (
  select
    ('N' || osm_id::string) as feature_id,
    osm_id::string as osm_id,
    nullif(name,'')::string as name,

    tags:"addr:housenumber"::string as addr_housenumber,
    tags:"addr:street"::string      as addr_street,
    tags:"addr:postcode"::string    as addr_postcode,
    coalesce(tags:"addr:city"::string, tags:"addr:place"::string) as addr_city_or_place,

    coalesce(
      tags:"amenity"::string,
      tags:"shop"::string,
      tags:"tourism"::string,
      tags:"leisure"::string,
      tags:"office"::string,
      tags:"craft"::string,
      tags:"man_made"::string,
      tags:"emergency"::string,
      tags:"public_transport"::string,
      tags:"railway"::string,
      tags:"highway"::string,
      tags:"place"::string
    ) as poi_type,

    case
      when tags:"amenity" is not null then 'amenity'
      when tags:"shop" is not null then 'shop'
      when tags:"tourism" is not null then 'tourism'
      when tags:"leisure" is not null then 'leisure'
      when tags:"office" is not null then 'office'
      when tags:"craft" is not null then 'craft'
      when tags:"man_made" is not null then 'man_made'
      when tags:"emergency" is not null then 'emergency'
      when tags:"public_transport" is not null then 'public_transport'
      when tags:"railway" is not null then 'railway'
      when tags:"highway" is not null then 'highway'
      when tags:"place" is not null then 'place'
      else null
    end as poi_class,

    tags as tags,
    other_tags::string as other_tags_raw,

    {{ wkt_to_geog('geom_wkt') }} as geog,
    region::string as region_code,
    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts
  from src
),

geo as (
  select
    *,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326
  from typed
  where geog is not null
    and poi_type is not null
),

final as (
  select *
  from geo
  {{ dedup_qualify(
      partition_by=['osm_id'],
      order_by=['load_ts desc','source_file desc']
  ) }}
)

select * from final