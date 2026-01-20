# geo_dbt

dbt project for the GEO pipeline (Snowflake). Builds curated **Silver** and **Gold** layers from **Bronze** sources, with tests + documentation in YAML.

## Architecture
- **BRONZE**: raw ingested tables (sources)
- **SILVER**: canonicalized/cleaned entities (dedup, normalized types, GEOGRAPHY/WKT debug fields)
- **GOLD**: modeling/feature-ready marts (H3 grids, aggregates, ML features)

## Requirements
- dbt (Cloud or Core) with Snowflake adapter
- Snowflake warehouse (e.g. `COMPUTE_WH`)
- Access to `GEO_PROJECT` database and schemas: `BRONZE`, `SILVER`, `GOLD`

## Project conventions
- Models are mostly `dynamic_table` with `target_lag = '48 hours'`
- Dedup is done via `QUALIFY ROW_NUMBER()` (latest `load_ts/source_file` wins)
- Geo debug is mandatory where applicable:
  - row count checks
  - WKT debug columns (e.g. `geom_wkt_4326`)
  - geometry validity checks (Snowflake-supported funcs only)

## Key macros
Located in `macros/`:
- `osm_tags_json(other_tags_col)` – parse OSM `other_tags` into JSON
- `wkt_to_geog(wkt_col)` – WKT → GEOGRAPHY (strict + allow-invalid fallback)
- `geog_to_wkt(geog_col)` – GEOGRAPHY → WKT
- `dedup_qualify(partition_by, order_by)` – standard dedup QUALIFY block
- H3 helpers (R10): point/centroid → cell

## Running (dbt Cloud / CLI)
Build a single model:
```bash
dbt build -s <model_name>
