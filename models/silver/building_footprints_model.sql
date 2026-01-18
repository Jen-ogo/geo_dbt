{{ config(materialized='dynamic_table', target_lag='48 hours', snowflake_warehouse='COMPUTE_WH') }}

select *
from {{ ref('building_footprints') }}
where lower(building_type) not in (
  'yes',
  'outbuilding','farm_auxiliary','shed','barn','sty','stable',
  'garage','garages','roof','greenhouse',
  'allotment_house',
  'hut','cabin'
)