{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH'
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','ROADS') }} b
  where b.osm_id is not null
),

typed as (
  select
    ('W' || osm_id::string) as feature_id,
    osm_id::string          as osm_id,
    nullif(name,'')::string as name,

    highway::string as highway,
    tags:"ref"::string as ref,

    coalesce(tags:"motorcar"::string, tags:"motor_vehicle"::string, tags:"vehicle"::string, tags:"access"::string) as access_raw,
    tags:"service"::string as service,

    lower(coalesce(tags:"oneway"::string, 'no')) as oneway_raw,
    iff(lower(coalesce(tags:"oneway"::string,'no')) in ('yes','true','1'), true, false) as oneway,

    {{ safe_num('tags:"lanes"::string') }}::int as lanes,

    tags:"surface"::string as surface,
    iff(lower(coalesce(tags:"lit"::string,'no')) in ('yes','true','1'), true, false) as lit,

    iff(lower(coalesce(tags:"bridge"::string,'no')) in ('yes','true','1'), true, false) as bridge,
    iff(lower(coalesce(tags:"tunnel"::string,'no')) in ('yes','true','1'), true, false) as tunnel,

    coalesce(
      {{ safe_num('tags:"layer"::string') }},
      {{ safe_num("regexp_substr(tags:\"layer\"::string, '^-?\\\\d+')") }},
      0
    )::int as layer,

    tags:"maxspeed"::string as maxspeed_raw,
    case
      when tags:"maxspeed"::string ilike '%mph%' then {{ safe_num("regexp_substr(tags:\"maxspeed\"::string, '\\\\d+')") }} * 1.60934
      else {{ safe_num("regexp_substr(tags:\"maxspeed\"::string, '\\\\d+')") }}
    end::number(10,2) as maxspeed_kph,

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
),

filtered as (
  select *
  from geo
  where highway is not null
    and lower(highway) not in ('footway','path','steps','corridor','bridleway','cycleway','pedestrian')
    and not (lower(highway)='service' and lower(coalesce(service,'')) in ('driveway','parking_aisle','alley','emergency_access','private'))
    and lower(coalesce(access_raw,'yes')) not in ('no','private')
  {{ dedup_qualify(
      partition_by=['osm_id'],
      order_by=['load_ts desc','source_file desc']
  ) }}
)

select * from filtered