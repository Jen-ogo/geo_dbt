-- Fail rows where cell WKT is not POLYGON/MULTIPOLYGON
-- Fail rows where cell WKT is not POLYGON/MULTIPOLYGON (Snowflake-safe, no regexp)
select
  region_code,
  h3_r10,
  cell_wkt_4326
from {{ ref('feat_h3_buildings_R10') }}
where
  cell_wkt_4326 is null
  or try_to_geography(cell_wkt_4326) is null
  or (
    upper(left(trim(cell_wkt_4326), 7)) <> 'POLYGON'
    and upper(left(trim(cell_wkt_4326), 12)) <> 'MULTIPOLYGON'
  )