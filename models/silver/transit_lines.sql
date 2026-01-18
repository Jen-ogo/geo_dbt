{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','PT_LINES') }} b
  where b.osm_id is not null
),

typed as (
  select
    ('W' || osm_id::string) as feature_id,
    osm_id::string as osm_id,

    case
      when railway is not null or tags:"railway" is not null then 'railway'
      when waterway is not null or tags:"waterway" is not null then 'waterway'
      when aerialway is not null or tags:"aerialway" is not null then 'aerialway'
      else null
    end as line_class,

    coalesce(
      railway::string, tags:"railway"::string,
      waterway::string, tags:"waterway"::string,
      aerialway::string, tags:"aerialway"::string
    ) as line_type,

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
    and line_class is not null
),

final as (
  select *
  from geo
  {{ dedup_qualify(partition_by=['osm_id'], order_by=['load_ts desc','source_file desc']) }}
)

select * from final