{{ config(materialized='table') }}

with
params as (
  select
    {{ var('dac_top_candidates', 50) }}::int                    as top_candidates,
    ceil({{ var('dac_top_candidates', 50) }}::float / 3.0)::int as per_group,

    {{ var('dac_roads_per_candidate_area', 30) }}::int          as roads_per_candidate_area,

    {{ var('dac_roads_per_station', 30) }}::int                 as roads_per_station,
    {{ var('dac_ev_station_radius_m', 300) }}::int              as ev_station_radius_m
),

base as (
  select
    region_code,
    region,
    h3_r7,
    cell_center_wkt_4326,
    macro_score,
    degurba
  from {{ ref('feat_h3_macro_score_r7') }}
  where cell_center_wkt_4326 is not null
    and cell_center_wkt_4326 ilike 'POINT(%'
    and degurba in (1,2,3)
),

picked as (
  select b.*
  from base b
  join params p on 1=1
  qualify row_number() over (partition by degurba order by macro_score desc) <= p.per_group
),

candidates as (
  select p.*
  from picked p
  join params x on 1=1
  qualify row_number() over (order by macro_score desc) <= x.top_candidates
),

cand_cells as (
  select
    c.region_code,
    c.region,
    c.h3_r7,
    c.degurba,
    c.macro_score,
    d.cell_geog
  from candidates c
  join {{ ref('dim_h3_r7_cells') }} d
    on  d.region_code = c.region_code
    and d.region      = c.region
    and d.h3_r7       = c.h3_r7
),

/* Major roads only */
roads as (
  select
    r.feature_id,
    r.osm_id,
    r.region_code,
    r.region,
    r.highway,
    r.maxspeed_kph,
    r.lanes,
    r.geog as road_geog,
    st_centroid(r.geog) as road_centroid_geog,
    st_length(r.geog)   as road_len_m
  from {{ ref('road_segments') }} r
  where r.geog is not null
    and lower(r.highway) in (
      'motorway','motorway_link',
      'trunk','trunk_link',
      'primary','primary_link',
      'secondary','secondary_link',
      'tertiary','tertiary_link'
    )
),

/* ---------- 1) candidate_area scope ---------- */
candidate_area_roads as (
  select
    cc.region_code,
    cc.region,
    cc.h3_r7,
    cc.degurba,
    cc.macro_score,

    'candidate_area'::string as traffic_scope,
    false::boolean           as near_ev_station,
    null::string             as ev_station_id,

    rd.feature_id as road_feature_id,
    rd.osm_id     as road_osm_id,
    rd.highway,
    rd.maxspeed_kph,
    rd.lanes,
    rd.road_len_m,
    st_aswkt(rd.road_centroid_geog) as road_centroid_wkt_4326
  from cand_cells cc
  join roads rd
    on  rd.region_code = cc.region_code
    and rd.region      = cc.region
    and st_contains(cc.cell_geog, rd.road_centroid_geog)
  join params p on 1=1
  qualify row_number() over (
    partition by cc.region_code, cc.region, cc.h3_r7
    order by rd.road_len_m desc
  ) <= p.roads_per_candidate_area
),

/* ---------- TomTom EV stations mapped to candidate hexes ---------- */
tomtom_stations_in_candidate as (
  select
    cc.region_code,
    cc.region,
    cc.h3_r7,
    cc.degurba,
    cc.macro_score,

    d.tomtom_poi_id::string as ev_station_id,
    to_geography('POINT(' || d.lon::string || ' ' || d.lat::string || ')') as ev_geog,

    m.dist_m,
    m.rank_by_dist
  from cand_cells cc
  join {{ ref('map_candidate_tomtom_stations') }} m
    on  m.region_code = cc.region_code
    and m.region      = cc.region
    and m.h3_r7       = cc.h3_r7
  join {{ ref('dim_tomtom_ev_stations') }} d
    on d.tomtom_poi_id = m.tomtom_poi_id
  where d.lat is not null
    and d.lon is not null
    and m.rank_by_dist <= 10
    and st_contains(
      cc.cell_geog,
      to_geography('POINT(' || d.lon::string || ' ' || d.lat::string || ')')
    )
),

/* ---------- 2) station_centric scope (around TomTom stations) ---------- */
station_centric_roads as (
  select
    s.region_code,
    s.region,
    s.h3_r7,
    s.degurba,
    s.macro_score,

    'station_centric'::string as traffic_scope,
    true::boolean             as near_ev_station,
    s.ev_station_id::string   as ev_station_id,

    rd.feature_id as road_feature_id,
    rd.osm_id     as road_osm_id,
    rd.highway,
    rd.maxspeed_kph,
    rd.lanes,
    rd.road_len_m,
    st_aswkt(rd.road_centroid_geog) as road_centroid_wkt_4326
  from tomtom_stations_in_candidate s
  join roads rd
    on  rd.region_code = s.region_code
    and rd.region      = s.region
    and st_distance(s.ev_geog, rd.road_centroid_geog) <= (select ev_station_radius_m from params)
  join params p on 1=1
  qualify row_number() over (
    partition by s.region_code, s.region, s.h3_r7, s.ev_station_id
    order by rd.road_len_m desc
  ) <= p.roads_per_station
),

/* ---------- UNION + DEDUP ---------- */
unioned as (
  select * from candidate_area_roads
  union all
  select * from station_centric_roads
),

dedup as (
  select
    u.*
  from unioned u
  qualify row_number() over (
    partition by u.region_code, u.region, u.h3_r7, u.road_feature_id
    order by
      iff(u.traffic_scope = 'station_centric', 1, 0) desc,
      u.road_len_m desc,
      coalesce(u.ev_station_id, '') desc
  ) = 1
)

select *
from dedup