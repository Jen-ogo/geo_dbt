{{ config(materialized='table') }}

select
  cast(null as varchar)        as CHARGING_AVAILABILITY_ID,
  cast(null as timestamp_ntz)  as SNAPSHOT_TS,
  cast(null as varchar)        as CONNECTOR_TYPE,
  cast(null as float)          as POWER_KW,
  cast(null as number(38,0))   as TOTAL,
  cast(null as number(38,0))   as AVAILABLE,
  cast(null as number(38,0))   as OCCUPIED,
  cast(null as number(38,0))   as RESERVED,
  cast(null as number(38,0))   as UNKNOWN,
  cast(null as number(38,0))   as OUT_OF_SERVICE,
  cast(null as variant)        as RAW_AVAIL_JSON
where 1=0