{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','poi_class']
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','POI_POLYGONS') }} b
),

typed as (
  select
    /* stable key: prefer osm_id when present, else W<osm_way_id> */
    coalesce(nullif(osm_id,''), 'W' || osm_way_id::string)::string as feature_id,

    nullif(osm_id,'')::string     as osm_id,
    osm_way_id::string            as osm_way_id,
    nullif(name,'')::string       as name,

    case
      when coalesce(amenity::string,  tags:"amenity"::string)  is not null then 'amenity'
      when coalesce(shop::string,     tags:"shop"::string)     is not null then 'shop'
      when coalesce(tourism::string,  tags:"tourism"::string)  is not null then 'tourism'
      when coalesce(office::string,   tags:"office"::string)   is not null then 'office'
      when coalesce(leisure::string,  tags:"leisure"::string)  is not null then 'leisure'
      when coalesce(sport::string,    tags:"sport"::string)    is not null then 'sport'
      when coalesce(building::string, tags:"building"::string) is not null then 'building'
      when coalesce(landuse::string,  tags:"landuse"::string)  is not null then 'landuse'
      else null
    end::string as poi_class,

    coalesce(
      amenity::string,  tags:"amenity"::string,
      shop::string,     tags:"shop"::string,
      tourism::string,  tags:"tourism"::string,
      office::string,   tags:"office"::string,
      leisure::string,  tags:"leisure"::string,
      sport::string,    tags:"sport"::string,
      building::string, tags:"building"::string,
      landuse::string,  tags:"landuse"::string
    )::string as poi_type,

    {{ wkt_to_geog('geom_wkt') }} as geog,

    /* no COUNTRY in this bronze: use region as region_code */
    lower(country::string) as region_code,
    nullif(trim(region::string),'') as region,

    source_file::string                 as source_file,
    load_ts::timestamp_ntz              as load_ts,

    tags                                as tags,
    other_tags::string                  as other_tags_raw
  from src
),

geo as (
  select
    *,
    {{ geog_to_wkt('geog') }} as geom_wkt_4326,
    st_centroid(geog)         as centroid_geog,
    st_area(geog)             as area_m2
  from typed
  where geog is not null
    and poi_class is not null
    and poi_type is not null
    and region_code is not null and trim(region_code) <> ''
),

final as (
  select *
  from geo
  {{ dedup_qualify(
      partition_by=['region_code','feature_id'],
      order_by=['load_ts desc','source_file desc']
  ) }}
)

select * from final