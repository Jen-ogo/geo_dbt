{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r7']
) }}

with p as (
  select
    region_code::string as region_code,
    region::string      as region,
    h3_point_to_cell_string(geog, 7)::string as h3_r7,
    lower(poi_class::string) as poi_class,
    lower(poi_type::string)  as poi_type,
    load_ts::timestamp_ntz   as load_ts
  from {{ ref('poi_points') }}
  where region_code is not null
    and region is not null
    and geog is not null
    and poi_class is not null
),

agg as (
  select
    region_code,
    region,
    h3_r7,

    count(*)                  as poi_points_cnt,
    count(distinct poi_class) as poi_classes_cnt,
    count(distinct poi_type)  as poi_types_cnt,

    count_if(poi_class='amenity')            as amenity_cnt,
    count_if(poi_class='shop')               as shop_cnt,
    count_if(poi_class='tourism')            as tourism_cnt,
    count_if(poi_class='leisure')            as leisure_cnt,
    count_if(poi_class='office')             as office_cnt,
    count_if(poi_class='craft')              as craft_cnt,
    count_if(poi_class='man_made')           as man_made_cnt,
    count_if(poi_class='emergency')          as emergency_cnt,
    count_if(poi_class='public_transport')   as public_transport_cnt,
    count_if(poi_class='railway')            as railway_cnt,
    count_if(poi_class='highway')            as highway_cnt,
    count_if(poi_class='place')              as place_cnt,

    count_if(poi_type in ('parking','parking_entrance','bicycle_parking')) as parking_cnt,
    count_if(poi_type in ('fuel','charging_station','car_wash','car_rental','car_sharing','parking_space')) as mobility_services_cnt,
    count_if(poi_type in ('supermarket','convenience','mall','department_store','hardware','doityourself')) as retail_core_cnt,
    count_if(poi_type in ('restaurant','fast_food','cafe','bar','pub')) as food_cnt,
    count_if(poi_type in ('hotel','motel','hostel','guest_house','apartments')) as lodging_cnt,
    count_if(poi_type in ('hospital','clinic','doctors','pharmacy')) as health_cnt,

    max(load_ts) as last_load_ts
  from p
  where h3_r7 is not null
  group by 1,2,3
),

cells as (
  select
    region_code::string as region_code,
   region::string      as region,
    h3_r7::string       as h3_r7,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }}
  where region_code is not null
    and region is not null
    and h3_r7 is not null
)

select
  c.region_code,
  a.region,
  c.h3_r7,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  coalesce(a.poi_points_cnt, 0) as poi_points_cnt,
  coalesce(a.poi_classes_cnt, 0) as poi_classes_cnt,
  coalesce(a.poi_types_cnt, 0) as poi_types_cnt,

  coalesce(a.amenity_cnt, 0) as amenity_cnt,
  coalesce(a.shop_cnt, 0) as shop_cnt,
  coalesce(a.tourism_cnt, 0) as tourism_cnt,
  coalesce(a.leisure_cnt, 0) as leisure_cnt,
  coalesce(a.office_cnt, 0) as office_cnt,
  coalesce(a.craft_cnt, 0) as craft_cnt,
  coalesce(a.man_made_cnt, 0) as man_made_cnt,
  coalesce(a.emergency_cnt, 0) as emergency_cnt,
  coalesce(a.public_transport_cnt, 0) as public_transport_cnt,
  coalesce(a.railway_cnt, 0) as railway_cnt,
  coalesce(a.highway_cnt, 0) as highway_cnt,
  coalesce(a.place_cnt, 0) as place_cnt,

  coalesce(a.parking_cnt, 0) as parking_cnt,
  coalesce(a.mobility_services_cnt, 0) as mobility_services_cnt,
  coalesce(a.retail_core_cnt, 0) as retail_core_cnt,
  coalesce(a.food_cnt, 0) as food_cnt,
  coalesce(a.lodging_cnt, 0) as lodging_cnt,
  coalesce(a.health_cnt, 0) as health_cnt,

  coalesce(a.poi_points_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as poi_points_per_km2,
  coalesce(a.parking_cnt, 0)    / nullif(c.cell_area_m2 / 1e6, 0.0) as parking_per_km2,
  coalesce(a.mobility_services_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as mobility_services_per_km2,

  a.last_load_ts
from cells c
left join agg a
  on a.region_code = c.region_code
 and a.region      = c.region
 and a.h3_r7       = c.h3_r7