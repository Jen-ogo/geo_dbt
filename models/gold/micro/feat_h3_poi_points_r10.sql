{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with p as (
  select
    region_code::string as region_code,
    -- poi_points.geog is POINT; safest is to use GEOG directly (not WKT)
    h3_point_to_cell_string(geog, 10)::string as h3_r10,
    poi_class::string as poi_class,
    poi_type::string  as poi_type
  from {{ ref('poi_points') }}
  where region_code is not null
    and geog is not null
),

agg as (
  select
    region_code,
    h3_r10,
    /* totals */
    count(*)                                   as poi_points_cnt,
    count(distinct poi_class)                  as poi_classes_cnt,
    count(distinct poi_type)                   as poi_types_cnt,
    /* breakdown by class */
    count_if(poi_class = 'amenity')            as amenity_cnt,
    count_if(poi_class = 'shop')               as shop_cnt,
    count_if(poi_class = 'tourism')            as tourism_cnt,
    count_if(poi_class = 'leisure')            as leisure_cnt,
    count_if(poi_class = 'office')             as office_cnt,
    count_if(poi_class = 'craft')              as craft_cnt,
    count_if(poi_class = 'man_made')           as man_made_cnt,
    count_if(poi_class = 'emergency')          as emergency_cnt,
    count_if(poi_class = 'public_transport')   as public_transport_cnt,
    count_if(poi_class = 'railway')            as railway_cnt,
    count_if(poi_class = 'highway')            as highway_cnt,
    count_if(poi_class = 'place')              as place_cnt,
    /* EV-relevant “landmarks” by poi_type (robust, but not exhaustive) */
    count_if(poi_type in ('parking','parking_entrance','bicycle_parking')) as parking_cnt,
    count_if(poi_type in ('fuel','charging_station','car_wash','car_rental','car_sharing','parking_space')) as mobility_services_cnt,
    count_if(poi_type in ('supermarket','convenience','mall','department_store','hardware','doityourself')) as retail_core_cnt,
    count_if(poi_type in ('restaurant','fast_food','cafe','bar','pub')) as food_cnt,
    count_if(poi_type in ('hotel','motel','hostel','guest_house','apartments')) as lodging_cnt,
    count_if(poi_type in ('hospital','clinic','doctors','pharmacy')) as health_cnt
  from p
  where h3_r10 is not null
  group by 1,2
),

cell as (
  -- canonical H3 cell geometry + debug WKT
  select
    region_code::string as region_code,
    h3_r10::string      as h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
)

select
  a.region_code,
  a.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.poi_points_cnt,
  a.poi_classes_cnt,
  a.poi_types_cnt,
  a.amenity_cnt,
  a.shop_cnt,
  a.tourism_cnt,
  a.leisure_cnt,
  a.office_cnt,
  a.craft_cnt,
  a.man_made_cnt,
  a.emergency_cnt,
  a.public_transport_cnt,
  a.railway_cnt,
  a.highway_cnt,
  a.place_cnt,
  a.parking_cnt,
  a.mobility_services_cnt,
  a.retail_core_cnt,
  a.food_cnt,
  a.lodging_cnt,
  a.health_cnt,
  /* densities per km2 (by hex area) */
  a.poi_points_cnt / nullif(c.cell_area_m2 / 1e6, 0) as poi_points_per_km2,
  a.amenity_cnt     / nullif(c.cell_area_m2 / 1e6, 0) as amenity_per_km2,
  a.shop_cnt        / nullif(c.cell_area_m2 / 1e6, 0) as shop_per_km2,
  a.parking_cnt     / nullif(c.cell_area_m2 / 1e6, 0) as parking_per_km
from agg a
join cell c
  on c.region_code = a.region_code
 and c.h3_r10       = a.h3_r10
