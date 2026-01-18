select *
from {{ ref('feat_h3_buildings_R10') }}
where h3_r10 is null
   or not regexp_like(h3_r10, '^[0-9a-f]{15}$')