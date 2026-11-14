# CLAUDE.md — Hyrox Databricks Port agent guardrails

Repo-level shared memory for any AI agent working in this repository.

The RUNBOOK covers *how to rebuild*; this file covers *how not to break things*.

## System of record (do not invert)

- The Snowflake repo (`hyrox-tieredsla-pipeline`) is the system of record. Anything updated or pushed there gets mirrored here. (Standing rule, 2026-10-17.)
- This repo proves the pipeline is portable in some aspects and gives the semantic layer (Apache Ossie) a second consumer. It does not replace the Snowflake repo.

## Identity & grain (do not reinterpret)

- `result_id` is the grain of every silver/gold table: `source_result_id` when present, else `res_synth_` + sha256(`event_id|name|division|total_time`).
- `athlete_id` is best-effort person identity: sha256(`name|gender|nationality|age_group`), rule v1. Mass `UNKNOWN` values signal an upstream name-normalization bug, not a data gap.
- `season` is NULLABLE. Never default missing seasons to 8. The table name `HYROX_RAW_SEASON_8` is a legacy name, not a constraint.
- `source_result_id` is lineage only (~698K nulls observed). Never use it as a join key or grain.

## Gold ownership (dbt owns it)

- Exactly ONE writer may publish `gold_marketing.pacing_obt`: dbt's gold models (`dbt_project/models/gold_marketing/pacing_obt.sql`, `SELECT * FROM stg_hyrox_races`).
- The WAP script's Publish stage (`scripts/wap_transforms.py`) writes its verified staging set to `gold_marketing.pacing_obt_wap_verified` — never to `pacing_obt`.
- The Q1/Q2 gold tables (`q1_station_long`, `q1_station_variance`, `q2_station_degradation`, `q2_deviation_correlation`) are dbt models in `dbt_project/models/gold_marketing/` — same rule: dbt owns them.

## Ossie semantic model (do not hand-maintain)

- Never hand-edit `ossie/semantic_model.yaml` beyond AI-context annotations.
- Regenerate only from dbt semantic models: `dbt parse && ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o ossie/semantic_model.yaml`
- Semantic models live in `dbt_project/models/gold_marketing/semantic_models.yml` and are defined on gold marts.

## dbt toolchain (the Fusion / venv trap — do not re-learn this)

- dbt Fusion 2.0.8 is a standalone binary (`~/.local/bin/dbt`), not pipx/uv-managed. Stay on 2.0.8 mid-proof.
- The MetricFlow CLI (`mf` 0.15.0) lives in the venv `~/.venvs/mf`. Installing it pulls dbt-core 1.12.6 as a dependency.
- `dbt parse/run/build` OUTSIDE the mf venv; `mf` commands INSIDE only. Activating the venv shadows Fusion: `dbt parse` inside it runs 1.12.6 and fails with a FALSE time-spine error ("semantic layer requires a time spine model with granularity DAY or smaller").
- Never add a time-spine model to fix a Fusion parse. Q1's metric has no time dimension and Fusion parses fine without one.
- This repo DOES carry a time-spine model (`dbt_project/models/gold_marketing/metricflow_time_spine.sql` + `.yml`), added during the Phase 5 MetricFlow validation. Do not remove it, and do not confuse it with the rule above: the rule is about never adding one to fix a Fusion parse error, not about deleting the one that is here.
- Pin dbt-core + dbt-databricks as a matched pair before the first build (the Snowflake side's unpinned install pulled a beta dbt-core and broke snapshots).
- All `dbt sl` commands are the dbt-Cloud platform route and fail locally with dbt1001 (no `dbt_cloud.yaml`). Local semantic-layer proof is `mf validate-configs` / `mf list metrics` / `mf query`.
- Project venvs live in `~/.venvs/<name>`, never inside the repo. The Streamlit venv is `~/.venvs/hyrox-databricks` (Python 3.12.16).

## Workspace references (never hardcode)

- Never hardcode the workspace host or warehouse HTTP path in code or docs. They come from the warehouse Connection details at setup time, live in environment variables at runtime (`DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`), and appear as placeholders (`<DATABRICKS_WORKSPACE_HOST>`, `<SQL_WAREHOUSE_HTTP_PATH>`) in `dbt_project/profiles.yml`.
- The profiles template (`dbt_project/profiles.yml`) is OAuth. Copy it to `~/.dbt/profiles.yml` and fill in your own values; never commit a filled-in copy.
- Free Edition: serverless only, single 2X-Small SQL warehouse. Service-principal OAuth (M2M) may 401 on Free Edition before any permission is checked — that is the Free limit, not a grants problem. A PAT is the working answer for the Streamlit app only, not for dbt.

## Environment variables (exact names)

- Streamlit app: `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `DATABRICKS_AUTH_TYPE=pat`. Local preview mode reads `HYROX_LOCAL_DATA_DIR` instead and never touches Databricks.
- The PAT runs as the workspace user and carries that user's permissions. Created 2026-11-11, 90-day, comment `hyrox-streamlit-render`, expires ~2027-02-09. Rotate before expiry (reminder set for 2027-01-28); after rotating, update the Render env vars, verify the live app, then revoke the old PAT.
- Never paste the PAT value, the workspace host, or the HTTP path into issues, PRs, or docs.

## Schema changes (four places, not one)

A column addition/removal/rename must land in ALL of:

1. `sql/01_delta_ddl.sql` (Delta DDL)
2. `notebooks/01_ingest_bronze.py` (bronze landing — identity columns `result_id`/`athlete_id`/`updated_at`)
3. `dbt_project/models/silver/stg_hyrox_races.sql` and `dbt_project/models/gold_marketing/pacing_obt.sql` — plus `dbt_project/models/gold_marketing/semantic_models.yml` if the column feeds a metric
4. `ossie/semantic_model.yaml` — regenerated from dbt, never hand-edited (beyond AI-context annotations)

## Dashboards

- Streamlit IS the frontend: the app in `streamlit_app/` (mobile-first) runs on Render — a NEW Web Service for this repo (decided 2026-11-14), not a re-point of the Snowflake service. It reads the Q1/Q2 gold tables with a PAT; it never recomputes them. No viewer login.
- Local preview mode (`HYROX_LOCAL_DATA_DIR`) reads the preview CSVs instead of Databricks — verify charts there before a live deploy.
- Python 3.12 only (Render uses 3.12). Altair 5 fails on Python 3.14 — keep `altair>=5,<6` and do not edit `requirements.txt` for this.
- The Snowflake repo's version of the app is the same front end against Snowflake tables; its Render deployment is down since the Snowflake trial expired (~2026-11-09).
- Verify live by the footer: "Data source: Databricks" (not "local CSV preview"), Sled Pull 6.70, Q2 pooled n = 5,000, −1.02 / 1.37 / −0.16.

## Genie + MCP/RAG (scope B — decided 2026-11-13)

- Scope B: Genie space over the gold tables, Unity Catalog functions, and an eval harness. Step 0 workspace checks first; build order is metric views + definitions ADR → Genie curation → UC functions → eval harness.
- Do not build scope A or C without a decision, and do not present Step 0 checks as a build.
- The 7 ADRs are Proposed, not Accepted. Until they are accepted and copied into `docs/adr/`, do not treat them as decided.

## Lineage (two layers, do not conflate)

- Unity Catalog, here: built-in table and column lineage in Catalog Explorer for the port tables, including `pacing_obt`, Q1, and Q2. This is the lineage layer for the Databricks side; the image is `docs/databricks_lineage_pacing_obt_wap_verified.png`.
- Neo4j, Snowflake side: the custom schema lineage graph lives in the Snowflake repo (`scripts/neo4j_load_schema.py`), not here.

## Verification (how not to be fooled)

- Verify file contents from the raw file on disk or its raw URL, not from a rendered, patch, diff, or truncated view. The GitHub patch view strips anything in angle brackets — placeholders such as `<DATABRICKS_WORKSPACE_HOST>` show as empty there; check the raw file.
- For "is this file unchanged/correct" questions, verify mechanically (empty git diff, or a byte-level diff) and say plainly when only a read-through was done. Never advise closing or discarding an unsaved file on a read-through alone.
- Never predict the full git conflict list before seeing the hunks; never issue a pull or merge unless asked. Remind the user to commit and push at each checkpoint — workspace edits don't reach GitHub on their own.

## Working style

- Label steps by environment (Mac/VS Code vs Databricks UI). Mac dbt edits in VS Code, not the Databricks UI, via `dbt build`.
- Manual-first: the user's commits, pushes, deploys, and outward messages are his hands — give paste-ready drafts and exact steps.
- Deliberate-build: log setup steps as a runbook, document decisions and tradeoffs in the README as you go, mirror system-of-record changes exactly.

## Secrets

- Never commit `.env`. Credentials resolve from environment variables at runtime only.
- Never paste workspace hosts, HTTP paths, usernames, or passwords into issues, PRs, or docs.

<sub>Parts of this project, including documentation and analysis, were developed with assistance from Naru, an AI assistant.</sub>