{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH'
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','ADMIN') }} b
  where b.osm_id is not null
),

base as (
  select
    ('A' || osm_id::string) as feature_id,
    osm_id::string          as osm_id,

    nullif(name,'')::string as name,
    tags:"name:en"::string  as name_en,

    admin_level::int        as admin_level,
    boundary::string        as boundary,

    try_to_number(tags:"population"::string) as population,
    try_to_date(tags:"population:date"::string) as population_date,

    type::string            as type,

    tags                    as tags,
    other_tags::string      as other_tags_raw,

    try_to_geometry(geom_wkt) as geom,
    {{ wkt_to_geog('geom_wkt') }} as geog,
    {{ geog_to_wkt( wkt_to_geog('geom_wkt') ) }} as geom_wkt_4326,

    lower(country::string) as region_code,
    nullif(trim(region::string),'') as region,
    source_file::string     as source_file,
    load_ts::timestamp_ntz  as load_ts
  from src
)

select *
from base
where geog is not null
{{ dedup_qualify(
     partition_by=['osm_id'],
     order_by=['load_ts desc','source_file desc']
) }}