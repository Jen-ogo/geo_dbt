{{ config(
    materialized='dynamic_table',
    target_lag='48 hours',
    snowflake_warehouse='COMPUTE_WH',
    cluster_by=['region_code','h3_r10']
) }}

with r as (
  select
    region_code::string        as region_code,
    osm_id::string             as osm_id,
    highway::string            as highway,
    nullif(service::string,'') as service,
    oneway::boolean            as oneway,
    lanes::int                 as lanes,
    maxspeed_kph::number(10,2) as maxspeed_kph,
    lit::boolean               as lit,
    bridge::boolean            as bridge,
    tunnel::boolean            as tunnel,
    geog                       as geog,
    load_ts::timestamp_ntz     as load_ts
  from {{ ref('road_segments') }}
  where geog is not null
    and region_code is not null
    and osm_id is not null
),

typed as (
  select
    r.*,
    {{ h3_r10_from_geog_centroid('geog') }}::string as h3_r10,
    st_length(geog)                                  as length_m,
    case
      when lower(highway) in ('motorway','motorway_link') then 'motorway'
      when lower(highway) in ('trunk','trunk_link')       then 'trunk'
      when lower(highway) in ('primary','primary_link')   then 'primary'
      when lower(highway) in ('secondary','secondary_link') then 'secondary'
      when lower(highway) in ('tertiary','tertiary_link') then 'tertiary'
      when lower(highway) in ('residential','living_street') then 'residential'
      when lower(highway) = 'service' then 'service'
      else 'other'
    end as road_class
  from r
  where {{ h3_r10_from_geog_centroid('geog') }} is not null
),

agg as (
  select
    region_code,
    h3_r10,
    count(*)                                    as road_segments_cnt,
    sum(length_m)                               as road_length_m_sum,
    sum(iff(road_class='motorway',     length_m, 0)) as motorway_length_m_sum,
    sum(iff(road_class='trunk',        length_m, 0)) as trunk_length_m_sum,
    sum(iff(road_class='primary',      length_m, 0)) as primary_length_m_sum,
    sum(iff(road_class='secondary',    length_m, 0)) as secondary_length_m_sum,
    sum(iff(road_class='tertiary',     length_m, 0)) as tertiary_length_m_sum,
    sum(iff(road_class='residential',  length_m, 0)) as residential_length_m_sum,
    sum(iff(road_class='service',      length_m, 0)) as service_length_m_sum,
    avg(lanes)::float                       as lanes_avg,
    approx_percentile(lanes, 0.5)           as lanes_p50,
    avg(maxspeed_kph)::float                as maxspeed_kph_avg,
    approx_percentile(maxspeed_kph, 0.5)    as maxspeed_kph_p50,
    approx_percentile(maxspeed_kph, 0.9)    as maxspeed_kph_p90,
    avg(iff(oneway,1,0))::float             as oneway_share,
    avg(iff(lit,1,0))::float                as lit_share,
    count_if(bridge)                        as bridge_cnt,
    count_if(tunnel)                        as tunnel_cnt,
    max(load_ts)                            as last_load_ts
  from typed
  group by 1,2
)

select
  a.region_code,
  a.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,
  a.road_segments_cnt,
  a.road_length_m_sum,
  a.motorway_length_m_sum,
  a.trunk_length_m_sum,
  a.primary_length_m_sum,
  a.secondary_length_m_sum,
  a.tertiary_length_m_sum,
  a.residential_length_m_sum,
  a.service_length_m_sum,
  a.lanes_avg,
  a.lanes_p50,
  a.maxspeed_kph_avg,
  a.maxspeed_kph_p50,
  a.maxspeed_kph_p90,
  a.oneway_share,
  a.lit_share,
  a.bridge_cnt,
  a.tunnel_cnt,
  /* densities per km2 (use dim cell area) */
  a.road_segments_cnt / nullif(d.cell_area_m2 / 1e6, 0) as road_segments_per_km2,
  a.road_length_m_sum / nullif(d.cell_area_m2 / 1e6, 0) as road_length_m_per_km2,
  a.last_load_ts
from agg a
join {{ ref('dim_h3_r10_cells') }} d
  on d.region_code = a.region_code
 and d.h3_r10       = a.h3_r10
