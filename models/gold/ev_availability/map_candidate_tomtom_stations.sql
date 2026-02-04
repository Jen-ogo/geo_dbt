{{ config(materialized='table') }}

select
  cast(null as varchar)        as REGION_CODE,
  cast(null as varchar)        as REGION,
  cast(null as varchar)        as H3_R7,
  cast(null as float)          as QUERY_LAT,
  cast(null as float)          as QUERY_LON,
  cast(null as number(38,0))   as RADIUS_M,
  cast(null as varchar)        as TOMTOM_POI_ID,
  cast(null as float)          as DIST_M,
  cast(null as float)          as SCORE,
  cast(null as number(38,0))   as RANK_BY_DIST,
  cast(null as timestamp_ntz)  as LOAD_TS,
  cast(null as variant)        as RAW_SEARCH_JSON
where 1=0