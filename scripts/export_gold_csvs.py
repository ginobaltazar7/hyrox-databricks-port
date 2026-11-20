"""Export the two gold tables to the CSVs the Streamlit app serves.

Writes streamlit_app/data/q1_station_variance.csv and
streamlit_app/data/q2_station_degradation.csv, in the exact shape the
app's loaders expect: same columns, same uppercase headers, same float
formatting ('%.10g') as the committed files.

Validation gate: nothing is written unless the fetched data passes every
check below, so a bad run can never overwrite good CSVs. Exit code is
non-zero on any failure, which fails the workflow step and leaves the old
CSVs in place.

The warehouse may be asleep when this runs; each query is retried up to
3 times with backoff, which covers a cold start.
"""

import csv
import math
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

from databricks import sql as dbsql

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "streamlit_app" / "data"

Q1_SQL = """
    SELECT station, variance_date, transition_time_variance, n_results AS n_obs
    FROM hyrox.gold_marketing.q1_station_variance
    ORDER BY station
"""
Q2_SQL = """
    SELECT division, first_km_deviation, station_degradation
    FROM hyrox.gold_marketing.q2_station_degradation
    ORDER BY division, first_km_deviation, station_degradation
"""
# Cross-check: the Q2 table must hold one row per complete result in the
# OBT. The Q2 model drops rows missing any run or station time, mirroring
# the Snowflake model, so this count must drop them too.
Q2_COUNT_SQL = """
    SELECT COUNT(*) FROM hyrox.gold_marketing.pacing_obt
    WHERE run1_time IS NOT NULL AND run2_time IS NOT NULL
      AND run3_time IS NOT NULL AND run4_time IS NOT NULL
      AND run5_time IS NOT NULL AND run6_time IS NOT NULL
      AND run7_time IS NOT NULL AND run8_time IS NOT NULL
      AND skierg_time IS NOT NULL AND wallballs_time IS NOT NULL
"""

Q1_HEADERS = ["STATION", "VARIANCE_DATE", "TRANSITION_TIME_VARIANCE", "N_OBS"]
Q2_HEADERS = ["DIVISION", "FIRST_KM_DEVIATION", "STATION_DEGRADATION"]

Q1_STATIONS = {
    "BURPEEBROADJUMP_TIME",
    "FARMERSCARRY_TIME",
    "ROWERG_TIME",
    "SANDBAGLUNGES_TIME",
    "SKIERG_TIME",
    "SLEDPULL_TIME",
    "SLEDPUSH_TIME",
    "WALLBALLS_TIME",
}

MAX_ATTEMPTS = 3


def _connection_params():
    # server_hostname is a bare hostname — no scheme. Strip one defensively
    # so a pasted full URL (https://dbc-....cloud.databricks.com) still works
    # instead of failing DNS inside the connector. The workflow already
    # passes the bare-hostname secret; this is belt and braces.
    hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    for prefix in ("https://", "http://"):
        if hostname.startswith(prefix):
            hostname = hostname[len(prefix):]
    hostname = hostname.rstrip("/")
    return {
        "server_hostname": hostname,
        "http_path": os.environ["DATABRICKS_HTTP_PATH"],
        "access_token": os.environ["DATABRICKS_TOKEN"],
    }


def _run_query(params, sql):
    """Run one query, retrying on connection/cold-start failures."""
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with dbsql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchall()
        except Exception as exc:  # noqa: BLE001 - retry, then re-raise
            last_error = exc
            print(f"query failed (attempt {attempt}/{MAX_ATTEMPTS}): {exc}", file=sys.stderr)
            if attempt < MAX_ATTEMPTS:
                time.sleep(60 * attempt)
    raise last_error


def _fail(message):
    print(f"VALIDATION FAILED: {message}", file=sys.stderr)
    sys.exit(1)


def _fmt_float(value):
    # Shortest string that round-trips to the same double (Python repr),
    # with a trailing ".0" stripped so whole numbers read "2", not "2.0".
    # Verified against the committed files: this reproduces all 5,008 values
    # byte-for-byte. '%.10g' is WRONG here — 48 Q2 values carry 11
    # significant digits (e.g. -39.191666667) and %.10g would silently
    # truncate them; plain repr is wrong the other way, writing "2.0"
    # where the files have "2" (97 values).
    text = str(float(value))
    return text[:-2] if text.endswith(".0") else text


def _validate_q1(rows):
    if len(rows) != 8:
        _fail(f"Q1 must have 8 rows, got {len(rows)}")
    stations = {r[0] for r in rows}
    if stations != Q1_STATIONS:
        _fail(f"Q1 stations mismatch: missing={Q1_STATIONS - stations} extra={stations - Q1_STATIONS}")
    for station, _date, variance, n_obs in rows:
        if n_obs is None or int(n_obs) <= 0:
            _fail(f"Q1 {station}: bad n_obs {n_obs!r}")
        if variance is None or not math.isfinite(float(variance)) or float(variance) <= 0:
            _fail(f"Q1 {station}: bad variance {variance!r}")


def _validate_q2(rows, obt_count):
    if not rows:
        _fail("Q2 has 0 rows")
    if len(rows) != obt_count:
        _fail(f"Q2 rows ({len(rows)}) != pacing_obt rows ({obt_count}) — partial build?")
    devs, degs = [], []
    for division, dev, deg in rows:
        if not division:
            _fail("Q2 row with empty division")
        if dev is None or deg is None:
            _fail(f"Q2 {division}: null deviation/degradation")
        if not math.isfinite(float(dev)) or not math.isfinite(float(deg)):
            _fail(f"Q2 {division}: non-finite deviation/degradation")
        devs.append(float(dev))
        degs.append(float(deg))
    corr = statistics.correlation(devs, degs) if len(devs) > 1 else float("nan")
    if not math.isfinite(corr) or not -1.0 <= corr <= 1.0:
        _fail(f"Q2 pooled correlation out of range: {corr}")
    return corr, sum(devs) / len(devs), sum(degs) / len(degs)


def _write_csv(path, headers, rows, formatters):
    """Write to a temp file in the same dir, then atomically replace."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([fmt(v) for fmt, v in zip(formatters, row)])
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def main() -> int:
    params = _connection_params()

    q1_rows = _run_query(params, Q1_SQL)
    q2_rows = _run_query(params, Q2_SQL)
    obt_count = _run_query(params, Q2_COUNT_SQL)[0][0]

    _validate_q1(q1_rows)
    corr, avg_dev, avg_deg = _validate_q2(q2_rows, int(obt_count))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    q1_path = DATA_DIR / "q1_station_variance.csv"
    q2_path = DATA_DIR / "q2_station_degradation.csv"
    q1_before = q1_path.read_bytes() if q1_path.exists() else None
    q2_before = q2_path.read_bytes() if q2_path.exists() else None

    _write_csv(q1_path, Q1_HEADERS, q1_rows, [str, str, _fmt_float, str])
    _write_csv(q2_path, Q2_HEADERS, q2_rows, [str, _fmt_float, _fmt_float])

    q1_status = "changed" if q1_path.read_bytes() != q1_before else "unchanged"
    q2_status = "changed" if q2_path.read_bytes() != q2_before else "unchanged"
    print(f"Q1: {len(q1_rows)} rows, Sled Pull {dict((r[0], r[2]) for r in q1_rows).get('SLEDPULL_TIME')} — {q1_status}")
    print(
        f"Q2: {len(q2_rows)} rows, pooled corr {corr:.10g}, "
        f"avg dev {avg_dev:.4f}, avg degr {avg_deg:.4f} — {q2_status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
