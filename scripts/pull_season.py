"""Pull a full Hyrox season with pyrox and land it in the Databricks Volume.

This is the automated half of the chain: on a schedule (Mondays 02:00 UTC)
the runner pulls the season straight from the Hyrox data source, validates
it is a real parquet with the columns the ingest notebook expects, and
uploads it to the Volume ``/Volumes/hyrox/bronze/landing/<target_name>``,
overwriting what is there. The refresh workflow then picks it up — or right
away, if the workflow's "run_refresh" is left on.

Auth: DATABRICKS_HOST + DATABRICKS_TOKEN (the workspace PAT). The token
needs the ``files`` scope for the upload (and ``jobs`` for the refresh it
triggers) — see the README's setup section.

Usage:
    python scripts/pull_season.py --season 8 \
        --target-name hyrox_season_8.parquet

Proven 2026-11-19: ``PyroxClient().get_season(season=8, use_cache=False)``
pulled 715,728 rows x 51 columns in 61.7s and wrote a 34.6 MB parquet that
passes this script's validation. The pull runs on GitHub's runner, which has
direct internet like Colab — no proxy workarounds are needed here (a sandbox
quirk during the proof, where one HTTP library ignored the egress proxy,
does not apply to the runner).
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pyrox
from databricks.sdk import WorkspaceClient

VOLUME_DIR = "/Volumes/hyrox/bronze/landing"

MIN_ROWS = 5000  # the pipeline promises a 5,000-row sample; anything smaller is a mistake, not a sample

# Same column set as scripts/land_parquet.py — the columns the ingest
# notebook (notebooks/01_ingest_bronze.py) needs. Column names are
# lowercased and spaces replaced with underscores before this check,
# exactly as the proof pull did.
REQUIRED_COLUMNS = {
    "name",
    "gender",
    "nationality",
    "age_group",
    "event_id",
    "division",
    "total_time",
    "source_result_id",
    "run1_time",
    "skierg_time",
    "wallballs_time",
    "best_run_lap",
}


def _pull(season: int):
    """Fetch the full season as a DataFrame, with normalized column names."""
    client = pyrox.PyroxClient()
    df = client.get_season(season=season, use_cache=False)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df


def _validate(path: Path) -> int:
    """Refuse anything that is not a readable parquet with the right columns."""
    try:
        meta = pq.read_metadata(path)
    except Exception as exc:  # noqa: BLE001 - any read failure is a bad file
        print(f"VALIDATION FAILED: not a readable parquet: {exc}", file=sys.stderr)
        sys.exit(1)
    if meta.num_rows < MIN_ROWS:
        print(
            f"VALIDATION FAILED: parquet has {meta.num_rows:,} rows, need at least {MIN_ROWS:,}",
            file=sys.stderr,
        )
        sys.exit(1)
    columns = set(meta.schema.names)
    missing = REQUIRED_COLUMNS - columns
    if missing:
        print(
            f"VALIDATION FAILED: missing columns the ingest needs: {sorted(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)
    return meta.num_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=8, help="Hyrox season to pull (default: 8)")
    parser.add_argument(
        "--target-name",
        default="hyrox_season_8.parquet",
        help="File name in the Volume (default: hyrox_season_8.parquet)",
    )
    args = parser.parse_args()

    if "/" in args.target_name or "\\" in args.target_name:
        print("target-name must be a plain file name, no path", file=sys.stderr)
        return 1

    print(f"pulling season {args.season} ...")
    df = _pull(args.season)
    print(f"pulled {len(df):,} rows x {len(df.columns)} columns")

    tmp = Path(tempfile.gettempdir()) / args.target_name
    df.to_parquet(tmp)
    print(f"wrote {tmp} ({os.path.getsize(tmp) / 1e6:.1f} MB)")
    rows = _validate(tmp)
    print(f"validated: {rows:,} rows, all required columns present")

    w = WorkspaceClient()
    volume_path = f"{VOLUME_DIR}/{args.target_name}"
    with open(tmp, "rb") as f:
        w.files.upload(volume_path, f, overwrite=True)
    print(f"landed in the Volume: {volume_path}")
    tmp.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
