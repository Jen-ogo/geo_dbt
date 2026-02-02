# geo_dbt

dbt project for the GEO pipeline (**Snowflake**). Builds curated **SILVER** and **GOLD** layers from **BRONZE** sources with YAML tests + documentation.

> Aligned with the Databricks/Delta repo (`geo_dbt_databricks`) in entity semantics, H3 conventions, and QA approach.

---

## Architecture

- **BRONZE** — raw ingested sources (staging/lineage)
- **SILVER** — canonical entities  
  (dedup, normalized types, `GEOGRAPHY` + mandatory WKT debug fields)
- **GOLD** — feature marts  
  (H3 grids, aggregates, ML-ready features)

---

## GOLD layout: macro vs micro

### Macro (H3 R7)
Coarse grid marts used for **candidate discovery / ranking** (where it makes sense to place EV charging).

### Micro (H3 R10)
Fine grid marts used for **detailed scoring** inside shortlisted macro areas.

**Rule:** micro marts must be restricted by a **candidate set** produced by macro (to avoid computing dense grids for whole countries).

---

## Conventions

- **Materialization:** mostly `dynamic_table` with `target_lag = '48 hours'`
- **Dedup:** standard “latest wins” pattern via `QUALIFY ROW_NUMBER()` (`load_ts`, `source_file`)
- **Geo QA (mandatory for geotables):**
  - rowcount checks
  - WKT debug columns: `geom_wkt_4326`, `cell_wkt_4326`, `cell_center_wkt_4326`
  - WKT sanity checks (not empty, expected prefix: `POINT` / `POLYGON` / `MULTIPOLYGON`)
- **H3 conventions (critical):**
  - canonical H3 key type = **STRING**
  - prefer reusing existing H3 computations/aggregations (avoid recomputing the same H3 repeatedly)

---

## Shared macros & tests (do not duplicate)

The repo already contains reusable generic tests/macros. Common ones:

- Tests: `rowcount_gt_0`, `not_empty`, `is_h3_hex`, `non_negative`, `wkt_not_empty`, `wkt_prefix_any`, `values_in_or_null`
- Macros: `osm_tags_json`, `wkt_to_geog`, `geog_to_wkt`, `dedup_qualify`, H3 helpers (R10)

---

## Running

Build a model:
Run tests:
```bash
dbt build -s <model_name>
dbt test -s <selector_or_model>
