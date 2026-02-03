{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH'
) }}

with src as (
  select
    gisco_id::string        as gisco_id,
    cntr_code::string       as cntr_code,
    lau_id::string          as lau_id,
    lau_name::string        as lau_name,
    dgURBA::int             as degurba,
    fid::number(38,0)       as fid,
    year::string            as year,
    geom_wkt::string        as geom_wkt_raw,
    source_file::string     as source_file,
    load_ts::timestamp_ntz  as load_ts
  from {{ source('bronze','EUROSTAT_LAU_DEGURBA') }}
  where cntr_code is not null
    and lau_id is not null
    and year is not null
    and geom_wkt is not null
),

dedup as (
  select
    *,
    {{ wkt_to_geog('geom_wkt_raw') }} as geog
  from src
  {{ dedup_qualify(
      partition_by=['cntr_code','lau_id','year'],
      order_by=['load_ts desc','source_file desc']
  ) }}
),

geo as (
  select
    ('LAU:' || cntr_code || ':' || lau_id || ':' || year) as feature_id,
    gisco_id,
    cntr_code,
    lau_id,
    lau_name,
    degurba,
    fid,
    year,
    geom_wkt_raw,
    geog,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326,
    st_isvalid(geog) as is_valid,
    st_centroid(geog) as lau_centroid_geog,
    source_file,
    load_ts
  from dedup
  where geog is not null
),

admin4 as (
  select
    region_code::string as region_code,
    region::string      as region,
    geog                as admin_geog
  from {{ ref('admin_areas') }}
  where admin_level = 4
    and boundary = 'administrative'
    and geog is not null
),

final as (
  select
    g.feature_id,

    g.gisco_id,
    g.cntr_code,
    g.lau_id,
    g.lau_name,
    g.degurba,
    g.fid,
    g.year,

    a.region_code,
    a.region,

    g.geom_wkt_raw,
    g.geom_wkt_4326,
    g.geog,
    g.is_valid,

    g.source_file,
    g.load_ts
  from geo g
  left join admin4 a
    on st_contains(a.admin_geog, g.lau_centroid_geog)
)

select * from final