#!/usr/bin/env python3
"""Upload notebooks to Databricks CICD folder."""

import base64
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

WORKSPACE_PATH = "/Workspace/Users/zoltan+de2prep2026@nordquant.com/ceu-modern-data-platforms-databricks"
HOST = "https://dbc-b134e182-2f19.cloud.databricks.com"
SKIP_DIRS = {".venv", ".git", ".dev", "__pycache__"}

repo_root = Path(__file__).parent.parent

# Read token
with open(repo_root / ".env") as f:
    for line in f:
        if line.startswith("DATABRICKS_TOKEN="):
            token = line.strip().split("=", 1)[1]

client = WorkspaceClient(host=HOST, token=token)

# Find and upload notebooks
for path in repo_root.rglob("*.py"):
    if any(skip in path.parts for skip in SKIP_DIRS):
        continue
    with open(path) as f:
        if "Databricks notebook source" not in f.readline():
            continue

    rel_path = path.relative_to(repo_root)
    target = f"{WORKSPACE_PATH}/{str(rel_path)[:-3]}"

    with open(path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    try:
        client.workspace.mkdirs("/".join(target.split("/")[:-1]))
    except:
        pass

    client.workspace.import_(
        path=target, content=content, format=ImportFormat.SOURCE,
        language=Language.PYTHON, overwrite=True
    )
    print(f"Uploaded: {rel_path}")

print("Done!")
