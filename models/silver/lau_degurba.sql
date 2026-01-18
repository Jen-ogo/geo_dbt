{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

with dedup as (
  select
    gisco_id::string    as gisco_id,
    cntr_code::string   as cntr_code,
    lau_id::string      as lau_id,
    lau_name::string    as lau_name,
    dgURBA::int         as degurba,
    fid::number(38,0)   as fid,
    year::string        as year,
    geom_wkt::string    as geom_wkt_raw,
    {{ wkt_to_geog('geom_wkt') }} as geog,
    source_file::string as source_file,
    load_ts::timestamp_ntz as load_ts
  from {{ source('bronze','EUROSTAT_LAU_DEGURBA') }}
  where cntr_code is not null
    and lau_id is not null
    and geom_wkt is not null
  {{ dedup_qualify(
      partition_by=['cntr_code','lau_id','year'],
      order_by=['load_ts desc','source_file desc']
  ) }}
),

final as (
  select
    ('LAU:' || cntr_code || ':' || lau_id || ':' || year) as feature_id,
    *,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326,
    st_isvalid(geog) as is_valid
  from dedup
  where geog is not null
)

select * from final