{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','PT_POINTS') }} b
  where b.osm_id is not null
),

typed as (
  select
    ('N' || osm_id::string) as feature_id,
    osm_id::string as osm_id,
    coalesce(nullif(name,''), tags:"name"::string) as name,

    case
      when tags:"public_transport" is not null
        or tags:"railway" is not null
        or lower(tags:"highway"::string) in ('bus_stop','platform')
      then 'transport'
      when tags:"amenity" is not null then 'amenity'
      when tags:"emergency" is not null then 'emergency'
      else null
    end as poi_class,

    case
      when tags:"public_transport" is not null then tags:"public_transport"::string
      when tags:"railway" is not null then tags:"railway"::string
      when tags:"highway" is not null then tags:"highway"::string
      when tags:"amenity" is not null then tags:"amenity"::string
      when tags:"emergency" is not null then tags:"emergency"::string
      else null
    end as poi_type,

    {{ wkt_to_geog('geom_wkt') }} as geog,
    lower(country::string) as region_code,
    nullif(trim(region::string),'') as region,
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
    and poi_class is not null
),

final as (
  select *
  from geo
  {{ dedup_qualify(partition_by=['osm_id'], order_by=['load_ts desc','source_file desc']) }}
)

select * from final