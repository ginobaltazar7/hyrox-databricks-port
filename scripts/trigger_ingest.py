"""Submit notebooks/01_ingest_bronze.py as a one-time Databricks job run and
wait for it to finish.

The notebook runs on serverless compute: the submit payload carries no
cluster config, so Databricks picks serverless, and the notebook's own
``# /// script [tool.databricks.environment] environment_version = "2"``
header decides the serverless environment version (the Jobs API cannot set
that — it must live in the notebook file, and it does).

Auth: DATABRICKS_HOST + DATABRICKS_TOKEN (the workspace PAT).
Notebook path: DATABRICKS_NOTEBOOK_PATH — the workspace path of the synced
notebook, without the .py extension.

Exit code 0 only if the run ends SUCCESS. Anything else fails the workflow
step, and the old CSVs keep serving.
"""

import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, SubmitTask

TIMEOUT_SECONDS = 30 * 60
POLL_SECONDS = 30


def _state_str(value):
    """Enum or plain string -> its plain value (robust across SDK versions)."""
    return getattr(value, "value", value)


def main() -> int:
    notebook_path = os.environ["DATABRICKS_NOTEBOOK_PATH"]

    w = WorkspaceClient()
    run = w.jobs.submit(
        run_name="hyrox-weekly-ingest",
        tasks=[
            SubmitTask(
                task_key="ingest_bronze",
                notebook_task=NotebookTask(notebook_path=notebook_path),
            )
        ],
    )
    run_id = run.run_id
    print(f"submitted ingest run {run_id} for {notebook_path}")

    deadline = time.time() + TIMEOUT_SECONDS
    while True:
        r = w.jobs.get_run(run_id)
        life = _state_str(r.state.life_cycle_state)
        print(f"run {run_id}: {life}")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = _state_str(r.state.result_state)
            print(f"run {run_id} finished: {result}")
            if result == "SUCCESS":
                return 0
            print(
                f"ingest failed: life_cycle_state={life} result_state={result}",
                file=sys.stderr,
            )
            return 1
        if time.time() > deadline:
            print(
                f"ingest timed out after {TIMEOUT_SECONDS}s (run {run_id}); cancelling the run",
                file=sys.stderr,
            )
            try:
                w.jobs.cancel_run(run_id)
                print(f"cancelled run {run_id}")
            except Exception as exc:  # noqa: BLE001 - cancel is best-effort; the timeout is the failure
                print(f"could not cancel run {run_id}: {exc}", file=sys.stderr)
            return 1
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
