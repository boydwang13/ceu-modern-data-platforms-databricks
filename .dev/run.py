#!/usr/bin/env python3
"""Run a local notebook on Databricks serverless."""

import sys
import base64
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import NotebookTask, Task, JobEnvironment

WORKSPACE_PATH = "/Workspace/Users/zoltan+de2prep2026@nordquant.com/ceu-modern-data-platforms-databricks"
HOST = "https://dbc-b134e182-2f19.cloud.databricks.com"

if len(sys.argv) < 2:
    print("Usage: uv run python .dev/run.py <notebook.py>")
    sys.exit(1)

repo_root = Path(__file__).parent.parent
local_path = (repo_root / sys.argv[1]).resolve()
if not local_path.exists():
    print(f"File not found: {local_path}")
    sys.exit(1)

rel_path = local_path.relative_to(repo_root)
target = f"{WORKSPACE_PATH}/{str(rel_path)[:-3]}"

# Read token
with open(repo_root / ".env") as f:
    for line in f:
        if line.startswith("DATABRICKS_TOKEN="):
            token = line.strip().split("=", 1)[1]

client = WorkspaceClient(host=HOST, token=token)

# Upload notebook
print(f"Uploading: {rel_path}")
with open(local_path, "rb") as f:
    content = base64.b64encode(f.read()).decode()

try:
    client.workspace.mkdirs("/".join(target.split("/")[:-1]))
except:
    pass

client.workspace.import_(
    path=target, content=content, format=ImportFormat.SOURCE,
    language=Language.PYTHON, overwrite=True
)

# Run notebook
print(f"Running: {target}")
run = client.jobs.submit(
    run_name=f"Run {rel_path}",
    tasks=[Task(
        task_key="run",
        notebook_task=NotebookTask(notebook_path=target),
        environment_key="env"
    )],
    environments=[JobEnvironment(environment_key="env")]
)

run_id = run.run_id
print(f"Run ID: {run_id}")
print(f"URL: {HOST}/#job/{run_id}/run/1")
print()

# Wait and show status
while True:
    result = client.jobs.get_run(run_id=run_id)
    state = result.state.life_cycle_state.name
    print(f"Status: {state}")

    if state not in ['PENDING', 'RUNNING', 'TERMINATING']:
        break
    time.sleep(5)

print(f"\nResult: {result.state.result_state}")
if result.state.state_message:
    print(f"Message: {result.state.state_message}")
