# RUNBOOK — Rebuilding the Hyrox Databricks Environment from Code

Everything this pipeline needs — catalog, schemas, tables, transforms — lives in this repo as code, not just inside a Databricks workspace. If a workspace is lost, or you need a fresh environment for a demo or a clean check, you pull this repo, run the steps below, and you get the same platform back. Free Edition has no expiry, so a rebuild here is a fresh-workspace exercise, not a trial rescue.

## When to use this

- Fresh workspace for a demo, a collaborator, or clean-room verification
- Rebuilding after a workspace problem (the data is re-ingested from the parquet sample; nothing is backed up out of band)
- NOT needed when a trial expires — there is no trial here. Free Edition does not expire. (Contrast with the Snowflake repo's RUNBOOK, which exists because the Snowflake trial ended.)

## Prerequisites

1. A Databricks Free Edition workspace: sign up at databricks.com/learn/free-edition → "For Personal Use" → **Free Edition** (not the 14-day trial). Personal email, no card. If signup rejects your email ("unable to signup, try a different email"), that address already has a Databricks identity — sign in instead, or use a different personal email.
2. The Databricks CLI: `brew install databricks/tap/databricks`; verify with `databricks version`.
3. Auth: `databricks auth login` → "Create a new profile" → paste the workspace host URL from the browser address bar (`https://dbc-XXXX-XXXX.cloud.databricks.com`, no trailing slash) → browser OAuth → "Authenticated".
4. A `~/.dbt/profiles.yml`: copy the template at `dbt_project/profiles.yml` and fill in your own values (workspace host, warehouse HTTP path). Never commit a filled-in copy.

## Rebuild steps

0. **Check your workspace first:** confirm you are in the right workspace — the host URL in the browser address bar matches the one you authenticated with — and that the Serverless Starter Warehouse (2X-Small) exists.
1. **Pull the latest repo first.** Workspace → Create → Git Folder → paste the repo URL → Create. (Public repo: clone needs no credential; pushing back prompts GitHub OAuth once.)
2. **Provision infrastructure:** open `sql/01_delta_ddl.sql` and run it on the Serverless Starter Warehouse (2X-Small) → creates the `hyrox` catalog, the schemas, and the bronze table.
3. **Landing volume + file:** SQL Editor: `CREATE VOLUME IF NOT EXISTS hyrox.bronze.landing;` then land the full-season parquet — the pull workflow does this weekly (see "Weekly refresh" below); the manual path is Catalog → hyrox → bronze → landing → Upload → `hyrox_season_8.parquet`, or the land workflow. The notebook samples 5,000 rows from whatever is landed, so the file in the Volume is the full-season pull (~715K rows), not the 5,000-row sample.
4. **Ingest:** open `notebooks/01_ingest_bronze.py`, attach to **Serverless** compute (not the SQL warehouse — Python needs serverless), Run all → Delta bronze table. The notebook asserts `result_id` unique and non-null before writing — the grain assert. The notebook first draws a deterministic 5,000-row sample from the full-season file (hash of each row's identity fields, 5,000 smallest hashes) before split parsing and the identity rules, so bronze stays the 5,000-row sample.
5. **Transform:** run dbt **outside** the MetricFlow venv, with dbt Fusion 2.0.8 — never inside `~/.venvs/mf`. The venv's dbt-core 1.12.6 shadows Fusion and fails with a false time-spine error. `mf` commands go inside the venv only. Copy the template at `dbt_project/profiles.yml` to `~/.dbt/profiles.yml` first, if you haven't already. Pin dbt-core + dbt-databricks as a matched pair before the first build.

   `cd dbt_project && dbt build --profiles-dir ~/.dbt` — silver + gold plus tests. (Or the split form: `dbt run --profiles-dir ~/.dbt && dbt test --profiles-dir ~/.dbt`.)
6. **Verify:** row counts match — 5,000 rows; `athlete_id` populated (no nulls, no `UNKNOWN` swarm); station-split columns present; `result_id` unique.

   - `SELECT COUNT(*) FROM hyrox.bronze.hyrox_raw_season_8;`
   - `SELECT COUNT(*) FROM hyrox.silver.stg_hyrox_races;`
   - `SELECT COUNT(*) FROM hyrox.gold_marketing.pacing_obt;`

   - **Then check the frozen Q1/Q2 numbers, not just the counts.** `SELECT * FROM hyrox.gold_marketing.q1_station_variance;` must have 8 rows with Sled Pull at 6.70. `SELECT * FROM hyrox.gold_marketing.q2_station_degradation;` must have 5,000 rows. `SELECT * FROM hyrox.gold_marketing.q2_deviation_correlation;` must have 7 rows with the pooled row at n = 5,000, −1.02 / 1.37 / −0.16. The dbt tables must agree with these numbers before you call the rebuild done.

   These are the original sample's numbers. After the first refresh run lands a new draw, the numbers move — the app picks them up automatically, and the checks above need the new values from that run's export log.
7. **Apply the WAP gate:** `scripts/wap_transforms.py` is the ported write-audit-publish gate. Data is written unverified first and promoted to `gold_marketing.pacing_obt_wap_verified` only if the audit passes; it halts if anomalies exceed 5%, and it also halts if the staging set has 10 or fewer rows — too few to audit, never published as verified.
8. **Semantic layer:** dbt semantic models are defined in `dbt_project/models/gold_marketing/semantic_models.yml`. Validate them with `mf validate-configs` / `mf list metrics` / `mf query` — inside the `~/.venvs/mf` venv only. The Ossie export is `ossie/semantic_model.yaml` — regenerate it from dbt, don't hand-edit it.
9. **Streamlit app — local first.** (a) Preview: set `HYROX_LOCAL_DATA_DIR` to the folder with the Q1/Q2 preview CSVs and run the app — it reads CSVs, never touches Databricks. (b) Live: the app reads the frozen CSVs in `streamlit_app/data/` and never touches Databricks at request time — the footer reads "Data source: local CSV preview". The four `DATABRICKS_*` values are still set on Render, but they are the fallback path only (used when the `data/` folder is absent). Run from the `~/.venvs/hyrox-databricks` venv (Python 3.12): `streamlit run streamlit_app/app.py`. Verify the footer reads "Data source: local CSV preview", Sled Pull 6.70, Q2 pooled n = 5,000, −1.02 / 1.37 / −0.16.
10. **Streamlit app — Render.** Create a NEW Web Service on the `hyrox-databricks-port` repo — do not re-point the Snowflake service, which is needed again for the fresh-trial Snowflake restore. Build command: `pip install -r streamlit_app/requirements.txt`. Start command: `streamlit run streamlit_app/app.py --server.port $PORT --server.address 0.0.0.0`. `PYTHON_VERSION` = 3.12. Env vars: the four `DATABRICKS_*` values (PAT). Free plan. Verify live with the same three checks as step 9.
11. **PAT rotation.** The PAT was created 2026-11-11, 90-day, comment `hyrox-streamlit-render`, expires ~2027-02-09. Before expiry: create a new PAT with the same comment and the `jobs`, `workspace`, `files`, and `sql` scopes, update `DATABRICKS_TOKEN` in the Render env vars AND in the repo's GitHub Actions secrets, verify the live app, then revoke the old PAT. Reminder set for 2027-01-28. The PAT runs as the workspace user, so it carries that user's permissions.

## Weekly refresh (automated) — ongoing operations

The app serves frozen CSVs from `streamlit_app/data/`, so it never touches
Databricks at request time. A weekly GitHub Actions chain keeps those CSVs
fresh; a failed run leaves the old CSVs serving, so the app never breaks.

- **Pull** — Mondays 02:00 UTC, `.github/workflows/pull-season.yml`: the
  runner pulls the full Season 8 with pyrox-client (open internet, like
  Colab), validates the parquet (readable, all 12 required columns, at
  least 5,000 rows), and lands it in the Volume `hyrox.bronze.landing`,
  then triggers the refresh. No Colab step.
- **Refresh** — Mondays 03:00 UTC, `.github/workflows/refresh-data.yml`:
  submits the ingest notebook as a one-time Jobs API run on serverless
  (the run is cancelled if it times out after 30 minutes), runs
  `dbt build`, exports the two gold tables to CSVs with a validation gate,
  and commits only if the CSVs changed. Render redeploys on the commit.
- **Land by hand** — `.github/workflows/land-parquet.yml`: paste a direct
  download URL (a link-shared Drive file, or a raw URL from the Snowflake
  repo) and it downloads, validates, and lands the file, optionally
  triggering the refresh. This is the manual fallback only.

Secrets (repo Settings → Secrets and variables → Actions): `DATABRICKS_HOST`
(workspace URL with scheme, SDK only), `DATABRICKS_SERVER_HOSTNAME` (bare
hostname, SQL connector and dbt), `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`
(the PAT), and `DATABRICKS_NOTEBOOK_PATH` (workspace path of the ingest
notebook, no `.py`). The PAT needs the `jobs`, `workspace`, `files`, and
`sql` scopes — the token created for the Streamlit app only carried `sql`,
and a Jobs API call 403s without them (Settings → Developer → Access
Tokens → Manage → Update token). Never paste a secret value into an issue,
a PR, or these docs.

The notebook samples 5,000 rows from the full-season file by a hash of each
row's identity fields (5,000 smallest hashes), so the same source file
always yields the same sample regardless of how Spark partitions the read.
A refresh only changes the CSVs when the source file changes — but the
first refresh after the sample change moves the frozen numbers to the new
draw, and the gist README and verification files still quote the old ones
until refreshed.

First run (by hand, from the Actions tab): run the pull workflow once with
the defaults (season 8, target `hyrox_season_8.parquet`, run_refresh on).
Expect a reorder-only CSV diff on the first successful refresh — the export
sorts deterministically. If a run fails, read the failed step's log in the
Actions tab; the old CSVs keep serving, and re-running the workflow by hand
is safe (the ingest timeout cancels the abandoned run, so a retry cannot
race it).

## What this pipeline covers

- **Identity has two keys:** Every result has its own `result_id`, and every athlete gets an `athlete_id` built from their details. That's how we tell results and people apart.
- **Grain fidelity:** One row per result, per event, the grain that everything else is built on.
- **A safety gate for trustworthiness:** A WAP audit has to pass before data is marked verified. Bad data stays out of the verified tables.
- **Lineage from Unity Catalog:** Built-in table and column lineage in Catalog Explorer for the port tables — no custom graph required here. The Neo4j graph lives in the Snowflake repo.
- **A local loop for day-to-day work:** venv + CLI + VS Code on the Mac; the Databricks UI only for the SQL Editor, notebooks, and Catalog Explorer.

## What this repo does NOT cover (do not go looking for it here)

- Cortex Analyst — Snowflake-only. The Nov 19 demo runs from a recording, not from this repo.
- The Neo4j schema graph — lives in the Snowflake repo (`scripts/neo4j_load_schema.py`).
- A Databricks Jobs DAG — there is none; the scheduler is GitHub Actions (see "Weekly refresh" above), not a Jobs DAG. The Snowflake task chain (`sql/04_task_dag.sql`) has no Databricks Jobs equivalent here.
- Genie / MCP / RAG — scope B, decided 2026-11-13; Step 0 workspace checks first. Not part of this rebuild.

## Databricks Free Edition constraints (verified)

- Serverless only; single 2X-Small SQL warehouse. Auth is OAuth via `databricks auth login`, then `auth_type: oauth` in `dbt_project/profiles.yml` — for dbt. A Personal Access Token is in use for the Streamlit app on Render (created 2026-11-11, 90-day, comment `hyrox-streamlit-render`, expires ~2027-02-09; rotation reminder set for 2027-01-28). Service-principal OAuth (M2M) may 401 on Free Edition before any permission is checked — that is the Free limit, not a grants problem — so PAT is the working answer here until the workspace moves off Free. The PAT runs as the workspace user, so it carries that user's permissions.
- Catalog creation goes through SQL on the warehouse (the Free Edition metastore has no storage root, so CLI/API catalog creation fails).
- Fair-use quota: compute will shut down if Databricks detects a daily compute threshold is exceeded, however data and settings are kept.
- Serverless notebooks can only reach Databricks-approved internet domains, so test the Neo4j Bolt connection from a notebook before building the graph step. If it's blocked, fall back to running the sync from your laptop, pulling the data through the Databricks SQL connector.
- Signup terms reserve the right to train on workspace data. The Hyrox dataset is public scraped race results, so exposure is nil. Still, keep the client or proprietary data out of this workspace.

<sub>Parts of this project, including documentation and analysis, were developed with assistance from Naru, an AI assistant.</sub>