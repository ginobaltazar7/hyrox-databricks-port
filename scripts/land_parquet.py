"""Land a parquet in the Databricks Volume for the refresh job to ingest.

Flow: download the parquet from a URL (a GitHub raw URL, or a link-shared
Google Drive file), validate it is a real parquet with the columns the
ingest notebook expects, then upload it to the Volume
``/Volumes/hyrox/bronze/landing/<target_name>``, overwriting what is there.

Auth: DATABRICKS_HOST + DATABRICKS_TOKEN (the workspace PAT).

Usage:
    python scripts/land_parquet.py --url <direct-download-url> \
        --target-name hyrox_season_8.parquet

The Volume is not versioned: landing a file overwrites the previous one,
so keep the source file (Drive, or the repo you committed it to) as the
backup. The refresh workflow picks the landed file up on its next run.
"""

import argparse
import os
import sys
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq
from databricks.sdk import WorkspaceClient

VOLUME_DIR = "/Volumes/hyrox/bronze/landing"

MIN_ROWS = 5000  # the pipeline promises a 5,000-row sample; anything smaller is a mistake, not a sample

# Columns the ingest notebook (notebooks/01_ingest_bronze.py) needs. The
# split columns are checked loosely — the notebook's pre-flight inspects
# them — but the identity columns must be there or the grain assert fails.
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


def _download(url: str, dest: Path) -> int:
    """Download a URL to dest, following redirects (Drive uc links redirect)."""
    request = urllib.request.Request(url, headers={"User-Agent": "hyrox-refresh/1.0"})
    with urllib.request.urlopen(request, timeout=120) as response, open(dest, "wb") as f:
        size = 0
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            size += len(chunk)
    return size


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
    parser.add_argument("--url", required=True, help="Direct download URL of the parquet")
    parser.add_argument(
        "--target-name",
        default="hyrox_season_8.parquet",
        help="File name in the Volume (default: hyrox_season_8.parquet)",
    )
    args = parser.parse_args()

    if "/" in args.target_name or "\\" in args.target_name:
        print("target-name must be a plain file name, no path", file=sys.stderr)
        return 1

    tmp = Path(f"/tmp/{args.target_name}")
    size = _download(args.url, tmp)
    print(f"downloaded {size:,} bytes")
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
