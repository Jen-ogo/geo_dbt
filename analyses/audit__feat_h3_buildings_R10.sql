-- Rowcount
select count(*) as rows_feat_h3_buildings_r10
from {{ ref('feat_h3_buildings_R10') }};

-- WKT samples
select region_code, h3_r10, cell_wkt_4326, cell_center_wkt_4326
from {{ ref('feat_h3_buildings_R10') }}
where cell_wkt_4326 is not null
limit 10;