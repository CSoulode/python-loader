# python-loader

Main M3 backend repository in this workspace.

This repo owns:

- `go-server/`: the active server implementation
- `ddl.sql`, `views.sql`, `list_taggings.sql`: runtime SQL assets
- `protos/dataloader.proto`: shared protobuf contract for this repo
- legacy Python loader/client/server code that is still kept in-tree

In the current workspace layout, `python-loader/`, `MetaDataCube-Client_2024/`, and `vectorkv/` are sibling repositories. `vectorkv/` is not inside this repo.

For the authoritative cross-repo runtime and architecture baseline, read:

- `../docs/architecture/overview.md`
- `../docs/architecture/vector_design.md`
- `../docs/architecture/workspace_runtime.md`

For a repo-local, code-based explanation of the current implementation, read:

- `docs/PROJECT_EXPLANATION.zh-CN.md`

## Repository Layout

- `go-server/`: current Go gRPC + HTTP server
- `protos/`: protobuf source
- `ddl.sql`, `views.sql`: schema and browsing-state support SQL
- `client/`: legacy Python CLI/import-export tooling
- `server/`: legacy Python gRPC server
- `docs/`: repo-local documentation only

## Current Runtime Role

In the current workspace, the common full stack is:

1. PostgreSQL
2. `vectorkv/`
3. `python-loader/go-server/`
4. `MetaDataCube-Client_2024/server_media.py`
5. `MetaDataCube-Client_2024/`

`go-server` is the primary backend. It serves:

- gRPC browsing-state and loader APIs
- HTTP compatibility endpoints under `/api/*` for the Angular client
- vector-aware browsing-state integration through the sibling `vectorkv` service

## Quick Start

### Go server

```bash
cd /workspaces/m3-workspace/python-loader/go-server
make run
```

`make run` loads environment variables from `.env`. For the current workspace values and startup order, use `../docs/architecture/workspace_runtime.md`.

### Database schema

Create the base schema from this repo:

```bash
cd /workspaces/m3-workspace/python-loader
psql -U postgres -f ddl.sql <database_name>
psql -U postgres -f views.sql <database_name>
```

### Tests

General Go test run:

```bash
cd /workspaces/m3-workspace/python-loader/go-server
set -a
source .env
set +a
timeout 300 go test ./...
```

When server tests need the workspace database:

```bash
cd /workspaces/m3-workspace/python-loader/go-server
set -a
source .env
export PHASE_A_TEST_DATABASE_URL="postgres://postgres:root@db:5432/emm-cube?sslmode=disable"
set +a
timeout 300 go test ./server -run 'TestPhase(A|B|C|D1|D2|D4|E1)' -count=1
```

## Vector Search Boundary

Vector storage and ANN search live in the sibling repo `../vectorkv/`.

- setup and commands: `../vectorkv/README.md`
- shared vector design: `../docs/architecture/vector_design.md`

`python-loader/go-server` consumes vectorkv over gRPC and exposes the combined browsing-state behavior to clients.

## Protocol Buffers

Repo-root Python bindings script:

```bash
cd /workspaces/m3-workspace/python-loader/client
source ../generate_protos
```

Go bindings script:

```bash
cd /workspaces/m3-workspace/python-loader/go-server/dataloader
./generate_protos_go
```

## Legacy Code

`client/` and `server/` remain in the repo for compatibility and historical workflows, but they are not the primary implementation path in this workspace. Treat `go-server/` as the active server path unless a task explicitly targets the legacy Python code.
