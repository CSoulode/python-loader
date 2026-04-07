# Phase D Benchmark Tool

`benchmark/cmd/bench` runs the D3 experiment matrix against managed `vectorkv` and `go-server` processes.

## Inputs

- Dataset DSNs are read from environment variables declared by `--dataset-config`
- Default envs remain:
  - `BENCH_DB_URL_182K`
  - `BENCH_DB_URL_725K`
- `python-loader/go-server/.env`: base server runtime env
- `vectorkv/config/models.json`: vector model catalog

The tool refuses to run a selected dataset when its DSN is missing.

## Usage

```bash
cd /workspaces/m3-workspace/python-loader/go-server
BENCH_DB_URL_182K=postgres://... \
BENCH_DB_URL_725K=postgres://... \
go run ./benchmark/cmd/bench \
  --datasets=182k,725k \
  --experiments=exp2,exp1,exp3,exp4,exp5,exp6,exp7
```

Pilot dataset example:

```bash
cd /workspaces/m3-workspace/python-loader/go-server
BENCH_DB_URL_PILOT_94K=postgres://postgres:root@db:5432/emm-cube?sslmode=disable \
go run ./benchmark/cmd/bench \
  --dataset-config=pilot_94k:94346:BENCH_DB_URL_PILOT_94K \
  --datasets=pilot_94k \
  --experiments=exp2,exp1,exp3,exp4
```

Common flags:

- `--datasets=182k,725k`
- `--dataset-config=label:size:dsn_env[,label:size:dsn_env...]`
- `--experiments=exp1,exp2,...,exp7`
- `--output-root=/workspaces/m3-workspace/docs/experiments/phase_d`
- `--go-server-env-file=/workspaces/m3-workspace/python-loader/go-server/.env`
- `--vector-models-file=/workspaces/m3-workspace/vectorkv/config/models.json`

## Behavior

- The benchmark builds fresh `vectorkv` and `go-server` binaries into `docs/experiments/phase_d/.tmp/`.
- Services are started and stopped by the benchmark. `vectorkv` readiness uses gRPC health. `go-server` readiness uses `GET /api/vector/models`.
- Experiment 1 uses the full selectivity grid: `1%, 5%, 10%, 20%, 30%, 40%, 50%, 100%`.
- Experiment 2 uses a structured estimation matrix: `4 complexities × 7 selectivities × 2 repeats = 56` queries.
- Experiment 3 and Experiment 4 use representative mixed-query samples with target selectivities `5%, 10%, 30%`.
- Experiment 1-4 run on the default HNSW runtime.
- Experiment 5 rebuilds HNSW, IVFFlat, DiskANN, and no-index baselines.
- Experiment 6 rebuilds HNSW and IVFFlat with iterative-scan modes across `1%, 5%, 10%, 20%, 30%, 40%, 50%`.
- Experiment 7 keeps the query unfiltered (`selectivity=1.0`) and rebuilds SigLIP2 full/half HNSW plus full DiskANN indexes.

## Output

Raw CSVs, explain plans, analysis scripts, and figure targets live under [`docs/experiments/phase_d/`](/workspaces/m3-workspace/docs/experiments/phase_d).

All experiment CSVs now include both:

- `dataset_label`: logical dataset name such as `182k`, `725k`, or `pilot_94k`
- `dataset_size`: object/vector count used for plotting and filtering

Server-side benchmark events are emitted as `BENCH_METRIC` JSON lines into per-dataset log files such as:

- `docs/experiments/phase_d/logs/182k-go-server.log`
- `docs/experiments/phase_d/logs/pilot_94k-go-server.log`

The benchmark uses gRPC metadata key `x-bench-id` so client runs can be joined back to server timing logs.
