# HYROX Streamlit app — Databricks version

A mobile-first web page for the analyst questions in the main README, reading
from Databricks instead of Snowflake. Same charts, same numbers — the
Databricks Q1/Q2 models already verified against the Snowflake ones.

Cloned from the Snowflake repo's `streamlit_app/`, with one file `data.py` replaced. `app.py` remains unchanged except the footer link (now points at this
repo). `.streamlit/config.toml` is unchanged.

| Section | Source table | Chart |
| --- | --- | --- |
| Q1 Station time variance (elite and pro, pooled) | `hyrox.gold_marketing.q1_station_variance` (8 rows) | Ranked bar chart |
| Q2 First km deviation vs degradation | `hyrox.gold_marketing.q2_station_degradation` (5,000 rows) | Scatter with division filter |
| Q2 By division | `hyrox.gold_marketing.q2_deviation_correlation` (7 rows) | Table, shown as is |
| Q5 Registration audit | none | Data notes panel |

## Files

- `app.py`: page layout, charts and footnotes. Only works with pandas DataFrames.
- `data.py`: the only file that talks to Databricks. See the header comment there for the full auth notes.
- `requirements.txt`: Python dependencies.
- `.streamlit/config.toml`: theme and settings.

## Why this exists

In part to experiment which parts, logic, pipeline, dashboards of the  Snowflake Hyrox repo are transferable. 

Databricks Free Edition is perpetual (no expiry, no card), with one 2X-Small
SQL warehouse while Snowflake free tier only lives 30 days. This port is one way to keep the same front end alive
after the trial ends. The tables it reads are already built and verified in
this repo.

## Auth — test this first

Render is unattended, so the `databricks auth login` OAuth (user-to-machine)
will not work there. There are two options, and `data.py` supports both.
It picks service-principal OAuth first if those variables are set, otherwise
a PAT. Set `DATABRICKS_AUTH_TYPE` to `oauth-m2m` or `pat` to force one.

### 1. Service principal, OAuth M2M (preferred in general — may not work on Free Edition)

- Create a service principal in the workspace, generate an OAuth secret.
  Note the client ID (UUID / Application ID) and the secret — the secret is
  shown once.
- Grant it `CAN USE` on the SQL warehouse, and `USE CATALOG` / `USE SCHEMA` /
  `SELECT` on `hyrox.gold_marketing` only.
- Env vars on Render:
  `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`,
  `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
- Access tokens last 1 hour and refresh automatically via the SDK. The
  OAuth secret lasts up to 730 days — rotate it before it expires, or the
  app stops loading data with no warning.

**Free Edition warning — read before building on this:** Free Edition has
no access to the account console or account-level APIs, and service-principal
OAuth depends on that account-level identity infrastructure. A community
report of SP OAuth failing with 401 on Free Edition fits that explanation,
and no official doc states plainly that it works there. So SP M2M may not
work on this workspace at all. Test it before committing to it: try creating
the service principal and running one query with the M2M credentials from
your laptop. If that fails with 401 before any table permission is even
checked, that is the Free Edition limit, not a grants problem — do not keep
troubleshooting grants.

### 2. Personal access token (fallback — works on Free Edition, legacy status)

- Create it as the workspace admin: Settings > Developer > Access tokens.
- Env vars on Render:
  `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`
- Databricks calls PATs legacy and recommends OAuth where possible, but
  they still work, can live from 1 day to indefinitely, and are revoked
  automatically after 90 days unused. A PAT runs as you, with your
  permissions — it is not scoped read-only the way a service principal
  can be. Prefer a short lifetime and rotate it.
- Databricks' own guidance, if you must use a PAT, is to use one
  associated with a service principal instead of a workspace user — which
  loops back to option 1, so on Free Edition a user PAT is the practical
  fallback.

**Verdict:** SP M2M is the right answer on a paid workspace, and the code
is written for it first. On Free Edition, PAT is the likely working answer,
despite the legacy label. The app supports both, so you can start on PAT
today and switch to SP later by changing env vars only — no code change.

### Where the values come from

- Server hostname + HTTP path: SQL warehouse > Connection details
  (HTTP path looks like `/sql/1.0/warehouses/<warehouse-id>`).
- Never put any of these in code or the repo. Env vars on Render only.

## Render settings

- **Build command:** `pip install -r streamlit_app/requirements.txt`
- **Start command:** `streamlit run streamlit_app/app.py --server.port $PORT --server.address 0.0.0.0`
- **Python version:** 3.11 or 3.12 (set `PYTHON_VERSION` on Render if needed; use 3.12)

## Run locally

Use Python 3.11 or 3.12 locally too (a venv). On a Mac with Python 3.14,
Altair 5 fails on import — don't edit the `altair>=5,<6` pin to work around it.

Against Databricks (export the variables first):

```
pip install -r streamlit_app/requirements.txt
streamlit run streamlit_app/app.py
```

Local preview without Databricks (development only, not used on Render): put
`q1_station_variance.csv` and `q2_station_degradation.csv` in a folder outside the repo, then

```
HYROX_LOCAL_DATA_DIR=/path/to/folder streamlit run streamlit_app/app.py
```

In preview mode the correlation table is rebuilt from the CSV with the same logic as the dbt model.

## Test sequence (research-first — do these in order)

1. **Local preview first:** run with `HYROX_LOCAL_DATA_DIR` and the two CSVs.
   If the charts don't render here, the problem is not Databricks.
2. **Auth test from your laptop:** with the env vars exported, run the app
   against Databricks. Try SP M2M first; if it 401s before any table is
   touched, switch `DATABRICKS_AUTH_TYPE=pat` and retest with a PAT.
   Do not move to Render until step 2 passes locally.
3. **Render:** create the Web Service, set the env vars, deploy. Check the
   live page shows "Data source: Databricks" at the bottom, with Sled Pull
   at 6.70 and the pooled Q2 numbers at n = 5,000, −1.02 / 1.37 / −0.16.
   If it says "local CSV preview", it is reading the preview files, not Databricks.

## Behaviour worth knowing

- **Caching:** each table is queried at most once an hour, so page views don't wake the warehouse.
- **Q2 chart range:** the scatter's default view is first km deviation from -12 to +4 minutes (`Q2_X_DOMAIN` in `app.py`). All rows stay in the data and in the correlation table. The footnote counts the points outside the range for the selected division, so it reads "4 points fall outside the chart range." on the pooled view.
- **Division filter:** each division is shown on its own by default (Open first). The pooled view is labelled as pooled.
- **Small groups:** Elite and Elite Doubles (n = 29) show a warning.
- **Errors:** if Databricks is unreachable, visitors see a short message and the technical detail sits in a collapsed box.
- **Grants:** dbt table models are recreated on each run, which drops SELECT grants. Keep `grants={'select': [...]}` on the three models so the app's identity keeps access — for the service principal, grant it explicitly; for a PAT, it runs as you.

<sub>Parts of this project, including documentation and analysis, were developed with assistance from Naru, an AI assistant.</sub>
