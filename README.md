# hyrox-databricks-port

A Databricks port of the [hyrox-tieredsla-pipeline](https://github.com/ginobaltazar7/hyrox-tieredsla-pipeline)
(Snowflake) medallion pipeline — same data, same questions, second cloud.

Why it exists: one pipeline, two warehouses, one semantic model. The Snowflake
repo is the system of record; this repo proves the pipeline is portable and
gives the semantic layer (Ossie) a second consumer. Portfolio piece: "ported a
production-style medallion pipeline from Snowflake to Databricks."

## Architecture map (Snowflake → Databricks)

| Snowflake | Databricks |
|---|---|
| `RAW_BRONZE.HYROX_RAW_SEASON_8` | `bronze.hyrox_raw_season_8` (Delta) |
| `SILVER.stg_hyrox_races` (dbt incremental) | `silver.stg_hyrox_races` (Delta, dbt) |
| `GOLD_MARKETING.pacing_obt` | `gold_marketing.pacing_obt` (Delta, dbt) |
| SPCS dbt runner / Metabase | Databricks SQL + AI/BI dashboards |
| Snowpark `raw_ingest.py` | `notebooks/01_ingest_bronze.py` |

## Repo layout

```
sql/          Delta DDL (catalog, schemas, bronze/silver/gold tables)
notebooks/    Ingestion notebook (parquet → Delta bronze)
dbt/          dbt project (databricks adapter) — silver + gold models
ossie/        Vendored Ossie hub doc (lands here when the semantic layer is built)
```

## Phases

- [x] **0. Scaffold** — this repo: README, Delta DDL, ingestion notebook draft, dbt skeleton
- [ ] **1. Free Edition setup** — sign up (personal email, no card), `databricks auth login`
- [ ] **2. Bronze** — run DDL, run ingestion notebook → Delta bronze
- [ ] **3. Silver + Gold** — `dbt build` with the databricks adapter
- [ ] **4. WAP + audits** — port the write-audit-publish gate and data-quality checks
- [ ] **5. Semantic layer** — vendor the Ossie doc from the Snowflake repo, export Databricks semantic layer
- [ ] **6. Dashboards** — AI/BI dashboards for Q1–Q5

## Free Edition constraints (verified)

- Serverless only; single 2X-Small SQL warehouse. No PATs — auth is OAuth via
  `databricks auth login`, then `auth_type: oauth` in `dbt/profiles.yml`.
- Catalog creation goes through SQL on the warehouse (the Free Edition metastore
  has no storage root, so CLI/API catalog creation fails).
- Fair-use quota: if you burn through it, compute shuts down for the rest of the
  day — data and settings are kept. Pace the build across days.
- Outbound internet from serverless is restricted to trusted domains: test Neo4j
  bolt reachability from a notebook early; laptop-via-SQL-connector is the fallback.
- Signup terms reserve the right to train on workspace data. The Hyrox dataset is
  public scraped race results, so exposure is nil — still, keep client or
  proprietary data out of this workspace.

## Getting started

```bash
git init && git add . && git commit -m "scaffold: Databricks port of hyrox pipeline"
gh repo create hyrox-databricks-port --public --source=. --push
```

Then work the phases top to bottom. The 10-hour plane block is sized for phases 1–4.
