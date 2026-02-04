{{ config(materialized='table') }}

select
  cast(null as varchar)        as TOMTOM_POI_ID,
  cast(null as varchar)        as CHARGING_AVAILABILITY_ID,
  cast(null as varchar)        as NAME,
  cast(null as varchar)        as BRAND,
  cast(null as varchar)        as CATEGORY,
  cast(null as varchar)        as COUNTRY_CODE,
  cast(null as varchar)        as COUNTRY_SUBDIVISION,
  cast(null as varchar)        as MUNICIPALITY,
  cast(null as varchar)        as STREET,
  cast(null as varchar)        as STREET_NUMBER,
  cast(null as varchar)        as POSTAL_CODE,
  cast(null as varchar)        as FREEFORM_ADDRESS,
  cast(null as float)          as LAT,
  cast(null as float)          as LON,
  cast(null as varchar)        as CONNECTORS_STATIC_JSON,
  cast(null as variant)        as RAW_POI_JSON,
  cast(null as timestamp_ntz)  as UPDATED_AT
where 1=0