select *
from {{ ref('feat_h3_buildings_R10') }}
where cell_center_wkt_4326 is not null
  and (
    try_to_geography(cell_center_wkt_4326) is null
    or upper(left(trim(cell_center_wkt_4326), 6)) <> 'POINT('
  )