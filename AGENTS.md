# AGENTS.md

This repository is the main M3 development repo in the workspace.

## Scope

Own the core implementation and runtime assets for:

- `go-server/` server behavior
- browsing-state computation, HTTP compatibility, and streaming behavior
- root-level runtime SQL such as `ddl.sql` and `views.sql`
- `protos/dataloader.proto`
- repo-local historical and compatibility material

## Internal Structure

Important directories and files:

- `go-server/`: active Go server implementation
- `client/`: legacy Python CLI and import/export tooling
- `server/`: legacy Python gRPC server
- `protos/`: shared protobuf definitions used by this repo
- `ddl.sql`, `views.sql`, `list_taggings.sql`: runtime SQL assets
- `docs/PROJECT_EXPLANATION.zh-CN.md`: repo-local explanation of this repo

## Legacy Within This Repo

Treat these as repo-internal legacy directories unless a task explicitly targets them:

- `client/`
- `server/`

Do not assume they are the current primary implementation path.

## Boundaries

- global architecture and paper notes belong in the workspace root `docs/`
- `MetaDataCube-Client_2024` owns the Angular client
- `vectorkv` owns vector storage, ANN search, and vector schema ensure
- this repo owns the main server integration path through `go-server/`

## Working Rules

- keep runtime SQL and `protos/` in this repo
- do not treat the workspace root `docs/sql/` as the runtime source of truth
- when a task touches vector semantics, read the root architecture docs first
- when a task touches hybrid integration, check both this repo and `vectorkv`

## Docs

Keep `docs/` repo-local.
The default canonical repo-local explanation is `docs/PROJECT_EXPLANATION.zh-CN.md`.
Do not keep global design papers or cross-repo design docs here.

## Codex Policy

Do not commit `.codex/` in this repo.
