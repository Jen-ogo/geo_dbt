SELECT DISTINCT
    region_code,
    h3_r10,
    H3_CELL_TO_BOUNDARY(h3_r10)          AS cell_geog,
    ST_ASWKT(H3_CELL_TO_BOUNDARY(h3_r10)) AS cell_wkt_4326,
    ST_AREA(H3_CELL_TO_BOUNDARY(h3_r10))  AS cell_area_m2,
    H3_CELL_TO_POINT(h3_r10)            AS cell_center_geog,
    ST_ASWKT(H3_CELL_TO_POINT(h3_r10))   AS cell_center_wkt_4326
  FROM {{ ref('gold_select_feat_h3_buildings_R10') }}