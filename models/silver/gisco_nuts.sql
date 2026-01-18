{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['level','cntr_code','nuts_id']
) }}

with base as (
  select
    nuts_id::string    as nuts_id,
    cntr_code::string  as cntr_code,
    name_latn::string  as name_latn,
    levl_code::string  as levl_code,
    level::int         as level,

    year::string       as year,
    scale::string      as scale,
    crs::string        as crs,

    geom_wkt::string   as geom_wkt_4326_raw,

    source_file::string      as source_file,
    load_ts::timestamp_ntz   as load_ts
  from {{ source('bronze','GISCO_NUTS') }}
  where nuts_id is not null
    and cntr_code is not null
    and level is not null
    and geom_wkt is not null
),
dedup as (
  select *
  from base
  qualify row_number() over (
    partition by level, cntr_code, nuts_id, year, scale, crs
    order by load_ts desc, source_file desc
  ) = 1
),
geo as (
  select
    ('NUTS:' || year || ':' || scale || ':' || crs || ':' || level::string || ':' || nuts_id) as feature_id,
    d.*,
    try_to_geography(d.geom_wkt_4326_raw)        as geog_strict,
    try_to_geography(d.geom_wkt_4326_raw, true)  as geog_allow,
    coalesce(
      try_to_geography(d.geom_wkt_4326_raw),
      try_to_geography(d.geom_wkt_4326_raw, true)
    ) as geog0
  from dedup d
)
select
  feature_id,
  nuts_id,
  cntr_code,
  name_latn,
  levl_code,
  level,
  year,
  scale,
  crs,
  geom_wkt_4326_raw as geom_wkt_4326,
  geog0             as geog,
  try_to_geometry(geom_wkt_4326_raw) as geom,
  source_file,
  load_ts
from geo
where geog0 is not null