{{ config(materialized='table') }}

with m as (
    select
      'feat_h3_buildings_R10' as model_name,
      count(*) as row_count,
      any_value(cell_wkt_4326) as wkt_polygon_sample,
      any_value(cell_center_wkt_4326) as wkt_point_sample
    from {{ ref('feat_h3_buildings_R10') }}

    union all
    select
      'cell_area_ref_feat_h3_buildings_R10',
      count(*),
      any_value(cell_wkt_4326),
      any_value(cell_center_wkt_4326)
    from {{ ref('cell_area_ref_feat_h3_buildings_R10') }}
)
select * from m;