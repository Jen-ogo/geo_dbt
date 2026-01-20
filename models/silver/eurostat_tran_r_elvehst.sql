{{ config(
    materialized = 'dynamic_table',
    target_lag   = '48 hours',
    snowflake_warehouse = 'COMPUTE_WH',
    cluster_by   = ['geo','year','vehicle']
) }}

with base as (
  select
    source_file::string       as source_file,
    snapshot::string          as snapshot,
    dataset::string           as dataset,
    freq::string              as freq,
    vehicle::string           as vehicle,
    unit::string              as unit,
    geo::string               as geo,
    year::int                 as year,
    value::number(38,0)       as value,
    ingest_ts::timestamp_ntz  as ingest_ts,
    load_ts::timestamp_ntz    as load_ts
  from {{ source('bronze','EUROSTAT_TRAN_R_ELVEHST') }}
  where geo is not null
    and year is not null
    and vehicle is not null
),
dedup as (
  select *
  from base
  qualify row_number() over (
    partition by geo, year, vehicle, unit, freq
    order by ingest_ts desc, load_ts desc, snapshot desc, source_file desc
  ) = 1
)
select
  md5(
    coalesce(geo,'') || '|' ||
    coalesce(year::string,'') || '|' ||
    coalesce(vehicle,'') || '|' ||
    coalesce(unit,'') || '|' ||
    coalesce(freq,'')
  ) as record_id,
  *
from dedup
