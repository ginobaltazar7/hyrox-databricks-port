# hyrox-databricks-port

A Databricks port of the [hyrox-tieredsla-pipeline](https://github.com/ginobaltazar7/hyrox-tieredsla-pipeline) (Snowflake) medallion pipeline using the same data, same questions, but a different cloud.

Why this port. To experiment and study the variations needed and patterns between one pipeline, one semantic model but differing warehouses. The Snowflake repo is the system of record; this repo proves the pipeline is portable in some aspects and gives the semantic layer (Ossie) a second consumer. 

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

- [x] **1. Free Edition setup** — sign up (personal email, no card), `databricks auth login`

- [x] **2. Bronze** — run DDL, run ingestion notebook → Delta bronze

- [ ] **3. Silver + Gold** — `dbt build` with the Databricks adapter

- [ ] **4. WAP + audits** — port the write-audit-publish gate and data-quality checks

- [ ] **5. Semantic layer** — vendor the Ossie doc from the Snowflake repo, export Databricks semantic layer

- [ ] **6. Dashboards** — AI/BI dashboards for Q1–Q5

## Setup runbook (first green build, 2026-10-17)

1. Sign up at databricks.com/learn/free-edition → "For Personal Use" → **Free Edition**
   (not the 14-day trial). If signup rejects your email ("unable to signup, try a
   different email"), that address already has a Databricks identity — sign in instead,
   or use a different personal email.

2. Install the CLI: `brew install databricks/tap/databricks`; verify with
   `databricks version`.

3. `databricks auth login` → "Create a new profile" → paste the workspace host URL
   from the browser address bar (`https://dbc-XXXX-XXXX.cloud.databricks.com`, no
   trailing slash) → browser OAuth → "Authenticated".

4. Workspace → Create → Git Folder → paste the repo URL → Create. (Public repo:
   clone needs no credential; pushing back prompts GitHub OAuth once.)

5. Open `sql/01_delta_ddl.sql` and run it on the Serverless Starter Warehouse
   (2X-Small) → creates the `hyrox` catalog, the schemas, and the bronze table.

6. SQL Editor: `CREATE VOLUME IF NOT EXISTS hyrox.bronze.landing;`

7. Catalog → hyrox → bronze → landing → Upload → `hyrox_season_8.parquet`.

8. Open `notebooks/01_ingest_bronze.py`, attach to **Serverless** compute (not the
   SQL warehouse — Python needs serverless), Run all → Delta bronze table.

## Deliberate decisions and tradeoffs

- **Snowflake repo is the system of record.** Anything updated or pushed in
  `hyrox-tieredsla-pipeline` gets mirrored here. (Standing rule, 2026-10-17.)

- **Thin bronze, on purpose.** The notebook lands all columns as-is from the
  parquet and only adds identity columns (`athlete_id`, `result_id`,
  `updated_at`). Per-column sanitization (split parsing, missing → NULL) is a
  follow-up: a PySpark port of the Snowflake `parse_split_minutes`, then a clean
  re-ingest (minutes on a 5K-row sample).

  While the Snowflake repo had Pydantic row-by-row validation in Python, the Spark-idomatic equivalent on Databricks, schema-on-read which is an explicit schema on `spark.read` did not seem needed. Porting Pydantic would have been unnecessary. (see Bronze carries light conformance point below and again in No Pydantic on Databricks side)

  Also, the load-bearing check at this stage is the grain: result_id unique and non-null, which the notebook asserts before writing. So electing to perform the validations at the latter stage of the Medallion, into Silver and Gold.

- **Identity recipe is exact-parity with Snowflake.** The first notebook draft
  used `sha256(event_id|athlete_id)` for synthetic `result_id`s; the Snowflake
  `raw_ingest.py` uses `sha256(event_id|name|division|total_time)` with a
  `res_synth_` prefix (16-char truncation), falling back to synthetic on null
  *or blank* source IDs, and `ath_` + 16-char `athlete_id`s. 
  
  The draft's recipe produced 2 duplicate `result_id`s on the sample (same athlete, same event, two races) that Snowflake's recipe distinguishes — caught by the grain assert. Fixed 2026-10-17; the assert now guards the parity.

- **Bronze carries light conformance — deliberately, for now.** Textbook
  medallion says bronze is raw and immutable, with dedupe and key derivation in
  silver, which was my original instinct to do. 
  
  Industry practice IMHO though and this is designed into the Snowflake pipeline this ports derives identity keys at bronze ingest. Refactoring to pure-raw bronze would
  ripple into the dbt silver models, which expect `result_id`/`athlete_id` to
  exist; deferred as a conscious call at the gate, not an oversight. The
  dupe-handling rule (quarantine vs. fail vs. dedupe) belongs to Phase 4
  (WAP + audits), where the write-audit-publish gate is its natural home.
  During the port, bronze asserts fail loud.

- **No Pydantic on the Databricks side.** Pydantic validates row-by-row in
  Python, which is upstream of how Spark works. The Spark-idiomatic equivalent is
  schema-on-read plus column checks and which lands with the sanitization
  follow-up. The load-bearing check today is the grain assert (`result_id`
  unique, non-null) before write.

- **Python notebooks run on Serverless compute, not the SQL warehouse.** The
  Starter Warehouse is SQL-only; the notebook's environment panel dependencies
  (simplejson, requirements.txt) are not needed and were skipped.

## Databricks Free Edition constraints (verified)

- Serverless only; single 2X-Small SQL warehouse. Personal Access Tokens is not used or needed at this time unless at some point a more automated workflow like CI/CD or third party tooling need to interact with the pipeline.  Auth is OAuth via `databricks auth login`, then `auth_type: oauth` in `dbt/profiles.yml`.

- Catalog creation goes through SQL on the warehouse (the Free Edition metastore
  has no storage root, so CLI/API catalog creation fails).

- Fair-use quota: compute will shut down if Databricks detects a daily compute threshold is exceeded, however data and settings are kept. 

- Serverless notebooks can only reach Databricks-approved internet domains, so test the Neo4j Bolt connection from a notebook before building the graph step. If it's blocked, fall back to running the sync from your laptop, pulling the data through the Databricks SQL connector.

- Signup terms reserve the right to train on workspace data. The Hyrox dataset is
  public scraped race results, so exposure is nil. Still, keep the client or
  proprietary data out of this workspace.

## Getting started

```bash
git init && git add . && git commit -m "scaffold: Databricks port of hyrox pipeline"
gh repo create hyrox-databricks-port --public --source=. --push
```

