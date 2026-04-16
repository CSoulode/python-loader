#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/workspaces/m3-workspace}"
GO_SERVER_DIR="${GO_SERVER_DIR:-$ROOT_DIR/python-loader/go-server}"
DATASET_LABEL="${DATASET_LABEL:-pilot_94k}"
DATASET_SIZE="${DATASET_SIZE:-94346}"
DATASET_DSN_ENV="${DATASET_DSN_ENV:-BENCH_DB_URL_PILOT_94K}"
BENCH_BIN="${BENCH_BIN:-/tmp/bench-future-experiment-framework}"
BENCH_GOCACHE="${BENCH_GOCACHE:-/tmp/go-build-bench-phase-f}"
GO_SERVER_BASE_ENV="${GO_SERVER_BASE_ENV:-$GO_SERVER_DIR/.env}"
GO_SERVER_ENV_FILE="${GO_SERVER_ENV_FILE:-/tmp/go-server.future-experiment-framework-phase-f.env}"

MAIN_ROOT="${MAIN_ROOT:-$ROOT_DIR/docs/experiments/future_experiment_framework_pilot_94k_20260411}"
AUDIT_ROOT="${AUDIT_ROOT:-$ROOT_DIR/docs/experiments/future_experiment_framework_pilot_94k_audit_20260411}"

if [[ -z "${!DATASET_DSN_ENV:-}" ]]; then
  printf 'missing required DSN env: %s\n' "$DATASET_DSN_ENV" >&2
  exit 1
fi

prepare_go_server_env() {
  if [[ -f "$GO_SERVER_ENV_FILE" ]]; then
    return
  fi
  awk '
    BEGIN { replaced = 0 }
    /^DB_HOST=/ { print "DB_HOST=db"; replaced = 1; next }
    { print }
    END {
      if (replaced == 0) {
        print "DB_HOST=db"
      }
    }
  ' "$GO_SERVER_BASE_ENV" >"$GO_SERVER_ENV_FILE"
}

build_bench() {
  (
    cd "$GO_SERVER_DIR"
    GOCACHE="$BENCH_GOCACHE" go build -o "$BENCH_BIN" ./benchmark/cmd/bench
  )
}

run_bench() {
  local output_root=$1
  env \
    "${DATASET_DSN_ENV}=${!DATASET_DSN_ENV}" \
    GOCACHE="$BENCH_GOCACHE" \
    "$BENCH_BIN" \
    --dataset-config="${DATASET_LABEL}:${DATASET_SIZE}:${DATASET_DSN_ENV}" \
    --datasets="$DATASET_LABEL" \
    --experiments="exp10,exp11" \
    --go-server-env-file="$GO_SERVER_ENV_FILE" \
    --output-root="$output_root"
}

run_analysis() {
  local output_root=$1
  (
    cd "$output_root/analysis"
    python3 analyze.py exp10 exp11
  )
}

require_phase_f_outputs() {
  local output_root=$1
  test -f "$output_root/raw/exp10_bs_cache_performance.csv"
  test -f "$output_root/raw/exp11_conditional_requests.csv"
  test -f "$output_root/figures/exp10_l0_l1_hit_rate.pdf"
  test -f "$output_root/figures/exp10_latency_by_path.pdf"
  test -f "$output_root/figures/exp10_l1_entries_vs_latency.pdf"
  test -f "$output_root/figures/exp11_not_modified_ratio.pdf"
  test -f "$output_root/figures/exp11_survival_rate.pdf"
}

main() {
  prepare_go_server_env
  build_bench
  run_bench "$MAIN_ROOT"
  run_analysis "$MAIN_ROOT"
  require_phase_f_outputs "$MAIN_ROOT"
  run_bench "$AUDIT_ROOT"
  run_analysis "$AUDIT_ROOT"
  require_phase_f_outputs "$AUDIT_ROOT"
}

main "$@"
