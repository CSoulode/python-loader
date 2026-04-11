#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/workspaces/m3-workspace}"
GO_SERVER_DIR="${GO_SERVER_DIR:-$ROOT_DIR/python-loader/go-server}"
BASE_OUTPUT_DIR="${BASE_OUTPUT_DIR:-$ROOT_DIR/docs/experiments}"
DATE_TAG="${DATE_TAG:-$(date -u +%Y%m%d)}"

DATASET_LABEL="${DATASET_LABEL:-pilot_94k}"
DATASET_SIZE="${DATASET_SIZE:-94346}"
DATASET_DSN_ENV="${DATASET_DSN_ENV:-BENCH_DB_URL_PILOT_94K}"

BENCH_BIN="${BENCH_BIN:-/tmp/bench-future-experiment-framework}"
BENCH_GOCACHE="${BENCH_GOCACHE:-/tmp/go-build-bench-main}"
GO_SERVER_BASE_ENV="${GO_SERVER_BASE_ENV:-$GO_SERVER_DIR/.env}"
GO_SERVER_ENV_FILE="${GO_SERVER_ENV_FILE:-/tmp/go-server.future-experiment-framework-${DATE_TAG}.env}"

MAIN_ROOT="${MAIN_ROOT:-$BASE_OUTPUT_DIR/future_experiment_framework_${DATASET_LABEL}_${DATE_TAG}}"
AUDIT_ROOT="${AUDIT_ROOT:-$BASE_OUTPUT_DIR/future_experiment_framework_${DATASET_LABEL}_audit_${DATE_TAG}}"
LOG_PATH="${LOG_PATH:-$MAIN_ROOT/logs/pilot-orchestrator.log}"

if [[ -z "${!DATASET_DSN_ENV:-}" ]]; then
  printf 'missing required DSN env: %s\n' "$DATASET_DSN_ENV" >&2
  exit 1
fi

mkdir -p "$MAIN_ROOT/logs" "$AUDIT_ROOT/logs"
exec > >(tee -a "$LOG_PATH") 2>&1

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

prepare_go_server_env() {
  if [[ -f "$GO_SERVER_ENV_FILE" ]]; then
    return
  fi
  if [[ ! -f "$GO_SERVER_BASE_ENV" ]]; then
    log "missing base go-server env file: $GO_SERVER_BASE_ENV"
    exit 1
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
  log "prepared go-server env file: $GO_SERVER_ENV_FILE"
}

build_bench_if_needed() {
  if [[ -x "$BENCH_BIN" && "${REBUILD_BENCH:-0}" != "1" ]]; then
    return
  fi
  log "build benchmark binary: $BENCH_BIN"
  (
    cd "$GO_SERVER_DIR"
    GOCACHE="$BENCH_GOCACHE" go build -o "$BENCH_BIN" ./benchmark/cmd/bench
  )
}

run_bench() {
  local output_root=$1
  local experiments=$2
  local dsn_value="${!DATASET_DSN_ENV}"
  log "run bench experiments=$experiments output_root=$output_root"
  env \
    "${DATASET_DSN_ENV}=${dsn_value}" \
    GOCACHE="$BENCH_GOCACHE" \
    "$BENCH_BIN" \
    --dataset-config="${DATASET_LABEL}:${DATASET_SIZE}:${DATASET_DSN_ENV}" \
    --datasets="$DATASET_LABEL" \
    --experiments="$experiments" \
    --go-server-env-file="$GO_SERVER_ENV_FILE" \
    --output-root="$output_root" \
    "${@:3}"
}

require_file() {
  local path=$1
  if [[ ! -f "$path" ]]; then
    log "missing expected file: $path"
    exit 1
  fi
}

require_lines() {
  local path=$1
  local expected=$2
  require_file "$path"
  local actual
  actual=$(wc -l <"$path")
  if [[ "$actual" -ne "$expected" ]]; then
    log "unexpected line count for $path: got $actual expected $expected"
    exit 1
  fi
  log "validated $path lines=$actual"
}

run_analysis() {
  local root=$1
  log "run analysis root=$root"
  (
    cd "$root/analysis"
    python3 analyze.py
  )
}

run_main_full() {
  run_bench "$MAIN_ROOT" exp2
  require_lines "$MAIN_ROOT/raw/exp2_selectivity_accuracy.csv" 57

  run_bench "$MAIN_ROOT" exp1
  require_lines "$MAIN_ROOT/raw/exp1_strategy_comparison.csv" 385

  run_bench "$MAIN_ROOT" exp3,exp4
  require_file "$MAIN_ROOT/raw/exp3_ssb_convergence.csv"
  require_file "$MAIN_ROOT/raw/exp4_cache_effect.csv"

  run_bench "$MAIN_ROOT" exp8
  require_lines "$MAIN_ROOT/raw/exp8_range_query.csv" 145

  run_bench "$MAIN_ROOT" exp9
  require_lines "$MAIN_ROOT/raw/exp9_range_filter_strategy.csv" 57

  run_bench "$MAIN_ROOT" exp5
  require_lines "$MAIN_ROOT/raw/exp5_index_comparison.csv" 769

  run_bench "$MAIN_ROOT" exp6
  require_lines "$MAIN_ROOT/raw/exp6_iterative_scan.csv" 36

  run_bench "$MAIN_ROOT" exp7
  require_lines "$MAIN_ROOT/raw/exp7_half_precision.csv" 13

  run_analysis "$MAIN_ROOT"
}

run_audit_full() {
  run_bench "$AUDIT_ROOT" exp2
  require_file "$AUDIT_ROOT/raw/exp2_selectivity_accuracy.csv"

  run_bench "$AUDIT_ROOT" exp1
  require_lines "$AUDIT_ROOT/raw/exp1_strategy_comparison.csv" 385

  run_bench "$AUDIT_ROOT" exp3,exp4
  require_file "$AUDIT_ROOT/raw/exp3_ssb_convergence.csv"
  require_file "$AUDIT_ROOT/raw/exp4_cache_effect.csv"

  run_bench "$AUDIT_ROOT" exp8
  require_lines "$AUDIT_ROOT/raw/exp8_range_query.csv" 145

  run_bench "$AUDIT_ROOT" exp9
  require_lines "$AUDIT_ROOT/raw/exp9_range_filter_strategy.csv" 57

  run_bench "$AUDIT_ROOT" exp5
  require_lines "$AUDIT_ROOT/raw/exp5_index_comparison.csv" 769

  run_bench "$AUDIT_ROOT" exp6
  require_lines "$AUDIT_ROOT/raw/exp6_iterative_scan.csv" 36

  run_bench "$AUDIT_ROOT" exp7
  require_lines "$AUDIT_ROOT/raw/exp7_half_precision.csv" 13

  run_analysis "$AUDIT_ROOT"
}

main() {
  prepare_go_server_env
  build_bench_if_needed
  log "orchestrator start"
  log "main and audit runs are executed sequentially, never concurrently"
  run_main_full
  run_audit_full
  log "orchestrator completed successfully"
}

main "$@"
