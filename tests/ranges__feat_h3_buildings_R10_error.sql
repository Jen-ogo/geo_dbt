-- Fail rows with impossible/invalid ranges
select
  region_code,
  h3_r10,
  cell_area_m2,
  buildings_cnt,
  buildings_per_km2,
  footprint_m2_per_km2,
  floor_area_m2_per_km2,
  built_up_share,
  levels_avg,
  levels_p50
from {{ ref('feat_h3_buildings_R10') }}
where 1=1
  -- base sanity
  and (
    cell_area_m2 is null or cell_area_m2 <= 0
    or buildings_cnt is null or buildings_cnt < 0

    -- derived densities must be >= 0 if present
    or (buildings_per_km2 is not null and buildings_per_km2 < 0)
    or (footprint_m2_per_km2 is not null and footprint_m2_per_km2 < 0)
    or (floor_area_m2_per_km2 is not null and floor_area_m2_per_km2 < 0)

    -- share should be bounded 
    --or (built_up_share is not null and (built_up_share < 0 or built_up_share > 5))

    -- optional sanity for levels
    or (levels_avg is not null and levels_avg < 0)
    or (levels_p50 is not null and levels_p50 < 0)
  )
