"""
Data access for the HYROX Streamlit app — Databricks version.

This is the only module that talks to the warehouse. The charts in app.py only
see pandas DataFrames with lowercase column names, so this file is the whole
port: app.py, requirements.txt (minus the Snowflake connector) and
.streamlit/config.toml are copied unchanged from the Snowflake repo.

Tables (all in the hyrox catalog, dbt models, materialised as Delta tables):
    hyrox.gold_marketing.q1_station_variance       8 rows
    hyrox.gold_marketing.q2_station_degradation    5,000 rows
    hyrox.gold_marketing.q2_deviation_correlation  7 rows

Column names differ slightly from the Snowflake models, so the SQL below
aliases them back to the names app.py expects:
    Q1:   n_results                  -> n_obs
    Q2:   (same three columns)
    Q2c:  n_results                  -> n
          avg_first_km_deviation     -> avg_deviation
          avg_station_degradation    -> avg_degradation
          correlation                -> corr_dev_vs_degradation

Authentication — two modes, both read-only at the warehouse level.
Set the mode with DATABRICKS_AUTH_TYPE, or leave it unset and let the file
pick: service-principal credentials win if present, otherwise a PAT.

  1. oauth-m2m (preferred, Databricks' recommendation for unattended apps)
     DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH,
     DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
     The service principal needs CAN USE on the SQL warehouse and
     USE CATALOG / USE SCHEMA / SELECT on hyrox.gold_marketing only.
     Access tokens last 1 hour and refresh automatically via the SDK.
     The OAuth secret itself lasts up to 730 days — rotate it before it
     expires, or the app stops loading data with no warning.
     WARNING — Free Edition: Databricks Free Edition has no access to the
     account console or account-level APIs, and service-principal OAuth
     depends on that account-level identity infrastructure. A community
     report of SP OAuth failing with 401 on Free Edition fits that
     explanation. SP M2M may not work on Free Edition at all — test it
     first (see README, "Auth — test this first"). If it fails, use PAT.

  2. pat (fallback — works on Free Edition, legacy status)
     DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
     Create the token as the workspace admin (Settings > Developer >
     Access tokens). Databricks calls PATs legacy and recommends OAuth
     where possible; they still work, can live from 1 day to indefinitely,
     and are revoked automatically after 90 days unused. A PAT runs as
     you, with your permissions — it is not scoped to read-only the way
     a service principal can be. Prefer a short lifetime and rotate it.
     If Databricks ever offers a PAT tied to a service principal in this
     workspace, use that instead of a user PAT.

Connection settings come from environment variables (set on Render,
never in code). Never commit a filled-in .env.

Local preview (development only): if HYROX_LOCAL_DATA_DIR points at a
folder with q1_station_variance.csv and q2_station_degradation.csv, the
app reads those instead of Databricks. This is not used on Render.
The loaders accept either column naming (n_obs or n_results, and the
long or short correlation names), so CSVs exported from either warehouse
work.
"""

import os
from pathlib import Path

import pandas as pd
import streamlit as st

CACHE_TTL_SECONDS = 60 * 60  # tables refresh daily; one query per table per hour at most

Q1_SQL = """
    SELECT station, transition_time_variance, n_results AS n_obs
    FROM hyrox.gold_marketing.q1_station_variance
"""

Q2_SQL = """
    SELECT division, first_km_deviation, station_degradation
    FROM hyrox.gold_marketing.q2_station_degradation
"""

Q2_CORR_SQL = """
    SELECT division,
           n_results AS n,
           avg_first_km_deviation AS avg_deviation,
           avg_station_degradation AS avg_degradation,
           correlation AS corr_dev_vs_degradation
    FROM hyrox.gold_marketing.q2_deviation_correlation
"""


class DataSourceError(RuntimeError):
    """Raised with a readable message when the app cannot load its data."""


# ---------------------------------------------------------------- helpers

def _local_dir():
    path = os.getenv("HYROX_LOCAL_DATA_DIR")
    return Path(path) if path else None


def _auth_type():
    """oauth-m2m if service-principal credentials are present, else pat."""
    explicit = os.getenv("DATABRICKS_AUTH_TYPE", "").strip().lower()
    if explicit in ("oauth-m2m", "m2m", "pat"):
        return "oauth-m2m" if explicit in ("oauth-m2m", "m2m") else "pat"
    if os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"):
        return "oauth-m2m"
    return "pat"


def missing_env_vars():
    base = ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH"]
    if _auth_type() == "oauth-m2m":
        base += ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
    else:
        base += ["DATABRICKS_TOKEN"]
    return [name for name in base if not os.getenv(name)]


def _credential_provider():
    """OAuth M2M credential provider, per the Databricks SQL connector docs.

    databricks-sql-connector >= 2.5.0 + databricks-sdk required.
    The SDK exchanges the client id/secret for a 1-hour token and
    refreshes it automatically.
    """
    from databricks.sdk.core import Config, oauth_service_principal

    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    config = Config(
        host=f"https://{server_hostname}",
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    return oauth_service_principal(config)


@st.cache_resource(show_spinner=False)
def _connection():
    from databricks import sql

    kwargs = {
        "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
    }
    if _auth_type() == "oauth-m2m":
        kwargs["credentials_provider"] = _credential_provider()
    else:
        kwargs["access_token"] = os.getenv("DATABRICKS_TOKEN")
    return sql.connect(**kwargs)


def _query(sql_text: str) -> pd.DataFrame:
    missing = missing_env_vars()
    if missing:
        raise DataSourceError("Missing environment variables: " + ", ".join(missing))
    try:
        conn = _connection()
        cur = conn.cursor()
        try:
            cur.execute(sql_text)
            rows = cur.fetchall()
            columns = [d[0].lower() for d in cur.description]
        finally:
            cur.close()
    except DataSourceError:
        raise
    except Exception as exc:  # noqa: BLE001
        # A cached connection can go stale; drop it so the next visit reconnects.
        _connection.clear()
        raise DataSourceError(f"Databricks query failed: {exc}") from exc
    return pd.DataFrame(rows, columns=columns)


def _read_local(name: str) -> pd.DataFrame:
    path = _local_dir() / name
    if not path.exists():
        raise DataSourceError(f"Local preview file not found: {path}")
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    return df


def data_source_label() -> str:
    return "local CSV preview" if _local_dir() else "Databricks"


# ---------------------------------------------------------------- public loaders

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading station data...")
def load_q1() -> pd.DataFrame:
    df = _read_local("q1_station_variance.csv") if _local_dir() else _query(Q1_SQL)
    # Accept either warehouse's column name in a local CSV.
    df = df.rename(columns={"n_results": "n_obs"})
    df = df[["station", "transition_time_variance", "n_obs"]].copy()
    df["transition_time_variance"] = df["transition_time_variance"].astype(float)
    df["n_obs"] = df["n_obs"].astype(int)
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading pacing data...")
def load_q2() -> pd.DataFrame:
    df = _read_local("q2_station_degradation.csv") if _local_dir() else _query(Q2_SQL)
    df = df[["division", "first_km_deviation", "station_degradation"]].copy()
    df["first_km_deviation"] = df["first_km_deviation"].astype(float)
    df["station_degradation"] = df["station_degradation"].astype(float)
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading correlation table...")
def load_q2_correlation() -> pd.DataFrame:
    if _local_dir():
        # Local preview only: rebuild the dbt table from the per result CSV with the
        # same logic as models/gold_marketing/q2_deviation_correlation.sql.
        q2 = load_q2()

        def summarise(frame, label):
            return {
                "division": label,
                "n": len(frame),
                "avg_deviation": frame["first_km_deviation"].mean(),
                "avg_degradation": frame["station_degradation"].mean(),
                "corr_dev_vs_degradation": frame["first_km_deviation"].corr(frame["station_degradation"]),
            }

        rows = [summarise(g, d) for d, g in q2.groupby("division")]
        rows.append(summarise(q2, "ALL"))
        df = pd.DataFrame(rows)
    else:
        df = _query(Q2_CORR_SQL)
    df = df.rename(columns={
        "n_results": "n",
        "avg_first_km_deviation": "avg_deviation",
        "avg_station_degradation": "avg_degradation",
        "correlation": "corr_dev_vs_degradation",
    })
    df["n"] = df["n"].astype(int)
    for col in ["avg_deviation", "avg_degradation", "corr_dev_vs_degradation"]:
        df[col] = df[col].astype(float)
    return df
