# ViRMA / VBS24 Legacy Setup Note

This file documents an older Python-server-based setup flow.

It is **not** the current workspace baseline.

Use these documents first for the current setup:

- `/workspaces/m3-workspace/docs/architecture/workspace_runtime.md`
- `/workspaces/m3-workspace/python-loader/README.md`
- `/workspaces/m3-workspace/python-loader/docs/PROJECT_EXPLANATION.zh-CN.md`

## What This File Still Applies To

Use the steps below only if you explicitly need the legacy Python `server/` and `client/` flow for historical data-loading or compatibility work.

## Legacy Database Setup

1. Install PostgreSQL.
2. Create the database:

```bash
createdb -U postgres VBS24
```

3. Load the base schema from `python-loader/`:

```bash
cd /workspaces/m3-workspace/python-loader
psql -U postgres -f ddl.sql VBS24
psql -U postgres -f views.sql VBS24
```

## Legacy Python gRPC Server

1. Go to `python-loader/server`.
2. Create a virtual environment.
3. Activate it.
4. Install requirements.
5. Update the database connection in `app.py`.
6. Run:

```bash
python app.py
```

## Legacy Loader Client

1. Go to `python-loader/client`.
2. Create and activate a virtual environment.
3. Install the client in editable mode:

```bash
pip install --editable .
```

4. Import the historical sample files with the `loader` CLI.

## Warning

This legacy path is not the main implementation path in the current workspace. The active backend is `python-loader/go-server`, and vector-aware runtime behavior depends on the sibling `vectorkv` repository rather than this older Python flow.
