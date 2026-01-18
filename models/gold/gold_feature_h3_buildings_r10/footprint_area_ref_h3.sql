SELECT
    region_code,
    h3_r10,
    building_levels,
    ST_AREA(geog)                        AS footprint_area_m2,
    CASE
      WHEN building_type IN (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) THEN 'residential'

      WHEN building_type IN (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) THEN 'nonresidential'

      WHEN building_type = 'yes' THEN 'unknown'
      ELSE 'other'
    END                                  AS building_group,
    load_ts
FROM {{ ref('gold_select_feat_h3_buildings_R10') }}