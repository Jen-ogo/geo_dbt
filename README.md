geo_dbt

dbt project for the GEO pipeline (Snowflake). Builds curated SILVER and GOLD layers from BRONZE sources, with YAML-based tests + documentation.

This repo is intentionally aligned with geo_dbt_databricks (Databricks/Delta) in:
	•	entity semantics in SILVER,
	•	H3 conventions (canonical H3 = STRING),
	•	QA approach (rowcount + mandatory WKT debug),
	•	GOLD split into macro (R7) and micro (R10) marts for EV siting.
Layers
	•	BRONZE: raw ingested sources (staging/lineage)
	•	SILVER: canonical entities (dedup, normalized types, GEOGRAPHY + WKT debug)
	•	GOLD: feature marts (H3 grids, aggregates, ML-ready features)

GOLD structure: macro vs micro
	•	GOLD / macro (H3 R7)
Coarse grid marts used for candidate discovery / ranking (where to build EV charging).
	•	GOLD / micro (H3 R10)
Fine grid marts used for detailed scoring inside shortlisted macro areas.

Rule: micro marts should be restricted by a candidate set (produced by macro) to avoid computing dense grids for whole countries.

Project conventions
	•	Materialization: mostly dynamic_table with target_lag = '48 hours'
	•	Dedup: standard “latest wins” via QUALIFY ROW_NUMBER() (by load_ts, source_file)
	•	Geo QA is mandatory for geotables
	•	rowcount checks
	•	WKT debug columns (e.g. geom_wkt_4326, cell_wkt_4326, cell_center_wkt_4326)
	•	geometry sanity tests (POINT/POLYGON prefix, WKT not empty)
	•	H3 conventions (critical)
	•	Canonical H3 key type is STRING
	•	Prefer reusing H3 computations/aggregations if already available upstream (avoid recomputing the same H3 repeatedly)

Shared macros & tests

The repo already contains reusable generic tests/macros (do not duplicate them). Common ones:
	•	rowcount_gt_0, not_empty
	•	is_h3_hex, non_negative
	•	wkt_not_empty, wkt_prefix_any, geog_is_point, geog_is_polygonal
	•	osm_tags_json, wkt_to_geog, geog_to_wkt, dedup_qualify
	•	H3 helpers (R10) like h3_r10_from_geog_point, h3_r10_from_geog_centroid
