{{ config(
    materialized = 'dynamic_table',
    target_lag   = '48 hours',
    snowflake_warehouse = 'COMPUTE_WH'
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','CHARGING') }} b
  where b.osm_id is not null
    and lower( ({{ osm_tags_json('b.other_tags') }}):"amenity"::string ) = 'charging_station'
),

base as (
  select
    ('N' || osm_id::string) as feature_id,
    osm_id::string          as osm_id,
    nullif(name,'')::string as name,

    tags:"name:en"::string  as name_en,
    tags:"amenity"::string  as amenity,
    tags:"operator"::string as operator,

    iff(lower(tags:"fee"::string) in ('yes','true','1'), true,
        iff(lower(tags:"fee"::string) in ('no','false','0'), false, null)
    ) as fee_bool,

    try_to_number(tags:"capacity"::string)::int as capacity,

    try_to_number(tags:"socket:type2"::string)::int       as socket_type2_cnt,
    try_to_number(tags:"socket:chademo"::string)::int     as socket_chademo_cnt,
    try_to_number(tags:"socket:type2_combo"::string)::int as socket_type2_combo_cnt,

    tags:"ref:EU:EVSE"::string      as ref_eu_evse,
    tags:"ref:EU:EVSE:pool"::string as ref_eu_evse_pool,

    {{ wkt_to_geog('geom_wkt') }} as geog,

    lower(country::string) as region_code,
    nullif(trim(region::string),'') as region,
    source_file::string   as source_file,
    load_ts::timestamp_ntz as load_ts
  from src
),

geo as (
  select
    *,
    st_aswkt(geog) as geom_wkt_4326
  from base
  where geog is not null
),

final as (
  select
    *,
    coalesce(socket_type2_cnt,0) + coalesce(socket_chademo_cnt,0) + coalesce(socket_type2_combo_cnt,0) as total_sockets_cnt,
    iff(coalesce(socket_chademo_cnt,0) + coalesce(socket_type2_combo_cnt,0) > 0, true, null) as has_dc,
    iff(coalesce(socket_type2_cnt,0) > 0, true, null) as has_ac
  from geo
  {{ dedup_qualify(partition_by=['osm_id'], order_by=['load_ts desc','source_file desc']) }}
)

select * from final