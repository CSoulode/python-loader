# Future Experiment Framework Benchmark Tool

`benchmark/cmd/bench` runs experiments `exp1` through `exp11` against managed `vectorkv` and `go-server` processes for the global experiment framework described in `docs/architecture/future_experiment_framework.md`.

## Inputs

- Dataset DSNs are read from environment variables declared by `--dataset-config`
- Default dataset envs remain:
  - `BENCH_DB_URL_182K`
  - `BENCH_DB_URL_725K`
- Pilot dataset example env:
  - `BENCH_DB_URL_PILOT_94K`
- `python-loader/go-server/.env`: base server runtime env
- `vectorkv/config/models.json`: vector model catalog

The tool refuses to run a selected dataset when its DSN is missing.

## Usage

Full matrix example:

```bash
cd /workspaces/m3-workspace/python-loader/go-server
BENCH_DB_URL_182K=postgres://... \
BENCH_DB_URL_725K=postgres://... \
go run ./benchmark/cmd/bench \
  --datasets=182k,725k \
  --experiments=exp2,exp1,exp3,exp4,exp8,exp9,exp5,exp6,exp7
```

Pilot example:

```bash
cd /workspaces/m3-workspace/python-loader/go-server
BENCH_DB_URL_PILOT_94K=postgres://postgres:root@db:5432/emm-cube?sslmode=disable \
go run ./benchmark/cmd/bench \
  --dataset-config=pilot_94k:94346:BENCH_DB_URL_PILOT_94K \
  --datasets=pilot_94k \
  --experiments=exp2,exp1,exp3,exp4,exp8,exp9,exp5,exp6,exp7,exp10,exp11
```

Phase F v2 full pilot main+audit example:

```bash
cd /workspaces/m3-workspace
BENCH_DB_URL_PILOT_94K=postgres://postgres:root@db:5432/emm-cube?sslmode=disable \
DATE_TAG=20260502 \
MAIN_ROOT=/workspaces/m3-workspace/docs/experiments/future_experiment_framework_pilot_94k_phase_f_v2_20260502 \
AUDIT_ROOT=/workspaces/m3-workspace/docs/experiments/future_experiment_framework_pilot_94k_phase_f_v2_audit_20260502 \
REBUILD_BENCH=1 \
python-loader/go-server/benchmark/scripts/run_future_experiment_framework_pilot.sh
```

Sequential main+audit helper:

```bash
cd /workspaces/m3-workspace
export BENCH_DB_URL_PILOT_94K=postgres://postgres:root@db:5432/emm-cube?sslmode=disable
python-loader/go-server/benchmark/scripts/run_future_experiment_framework_pilot.sh
```

This helper runs the main tree first, waits for it to finish, runs analysis, and only then starts the fresh audit tree. It does not run main and audit concurrently.

Common flags:

- `--datasets=182k,725k`
- `--dataset-config=label:size:dsn_env[,label:size:dsn_env...]`
- `--experiments=exp1,exp2,...,exp11`
- `--output-root=/workspaces/m3-workspace/docs/experiments/future_experiment_framework_pilot_94k_20260411`
- `--go-server-env-file=/workspaces/m3-workspace/python-loader/go-server/.env`
- `--vector-models-file=/workspaces/m3-workspace/vectorkv/config/models.json`

If `--output-root` is omitted, the tool derives a dataset-aware root name such as:

- `docs/experiments/future_experiment_framework_pilot_94k_<YYYYMMDD>/`
- `docs/experiments/future_experiment_framework_182k_<YYYYMMDD>/`
- `docs/experiments/future_experiment_framework_multi_<YYYYMMDD>/`

## Behavior

- The benchmark builds fresh `vectorkv` and `go-server` binaries into `<run_root>/.tmp/`.
- Services are started and stopped by the benchmark. `vectorkv` readiness uses gRPC health. `go-server` readiness uses `GET /api/vector/models`.
- Every new output tree is scaffolded with:
  - `README.md`
  - `environment.md`
  - `summary.md`
  - `report.md`
  - `comparison.md`
  - `diskann_label_feasibility.md`
  - a copied `analysis/` plotting bundle
- Experiment order defaults to:
  - `exp2`
  - `exp1`
  - `exp3`
  - `exp4`
  - `exp8`
  - `exp9`
  - `exp5`
  - `exp6`
  - `exp7`
- Experiment highlights:
  - `exp1`: strategy comparison for KNN plus range rows with `dist_min / dist_max`
  - `exp5`: index comparison for KNN plus range rows with `dist_min / dist_max`
  - `exp6`: records both generic `plan_used_index` and precise `plan_used_vector_ann_index`
  - `exp8`: range matrix across model, index type, query type, width, and position
  - `exp9`: range + metadata filtering with `query_type`, `ttfb_ms`, `ttlb_ms`, and `selected_strategy`
  - `exp10`: Phase F v2 state-chain performance, writing `raw/exp_f_chain_perf.csv`
  - `exp11`: Phase F v2 conditional request and invalidation validation, writing `raw/exp_f_chain_invalidation.csv`

## Output

Generated artifacts live under the chosen run root in `docs/experiments/`.

All experiment CSVs include both:

- `dataset_label`: logical dataset name such as `182k`, `725k`, or `pilot_94k`
- `dataset_size`: object/vector count used for plotting and filtering

Server-side benchmark events are emitted as `BENCH_METRIC` JSON lines into per-dataset log files such as:

- `<run_root>/logs/pilot_94k-go-server.log`
- `<run_root>/logs/182k-go-server.log`

The benchmark uses gRPC metadata key `x-bench-id` so client runs can be joined back to server timing logs.

## Validation Notes

- Source `python-loader/go-server/.env` before broader `go test ./...` runs; package init requires the ungrouped query env vars.
- Export `PHASE_A_TEST_DATABASE_URL` when running server tests that touch the workspace database.
- For real full-chain smoke, follow `docs/architecture/workspace_runtime.md`.
- Avoid wrapping real benchmark invocations in a blanket `timeout 300`; targeted unit tests should keep the 300s cap, but pilot benchmark preprocessing can exceed that before first artifacts appear.
- `benchmark/scripts/run_future_experiment_framework_pilot.sh` is the saved sequential orchestrator for future `pilot_94k` main+audit reruns.
