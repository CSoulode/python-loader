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
VALIDATOR="${VALIDATOR:-$GO_SERVER_DIR/benchmark/scripts/validate_future_experiment_framework.py}"
DOC_WRITER="${DOC_WRITER:-$GO_SERVER_DIR/benchmark/scripts/write_future_experiment_docs.py}"

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

require_matching_exp3_shape() {
  local main_path=$1
  local audit_path=$2
  require_file "$main_path"
  require_file "$audit_path"
  python3 "$VALIDATOR" exp3-shape "$main_path" "$audit_path"
  log "validated exp3 convergence shape matches between main and audit"
}

write_final_docs() {
  python3 "$DOC_WRITER" --root "$MAIN_ROOT" --pair "$AUDIT_ROOT" --role main --dataset "$DATASET_LABEL"
  python3 "$DOC_WRITER" --root "$AUDIT_ROOT" --pair "$MAIN_ROOT" --role audit --dataset "$DATASET_LABEL"
  log "updated final run docs for main and audit roots"
}

require_exp9_selected_strategy() {
  local path=$1
  require_file "$path"
  python3 "$VALIDATOR" exp9-selected-strategy "$path"
  log "validated exp9 auto selected_strategy values in $path"
}

require_phase_f_perf() {
  local path=$1
  require_lines "$path" 69
  python3 "$VALIDATOR" phase-f-perf "$path"
  log "validated phase F exp10 perf gates in $path"
}

require_phase_f_invalidation() {
  local path=$1
  require_lines "$path" 5
  python3 "$VALIDATOR" phase-f-invalidation "$path"
  log "validated phase F exp11 invalidation gates in $path"
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
  require_lines "$MAIN_ROOT/raw/exp3_ssb_convergence.csv" 241
  require_file "$MAIN_ROOT/raw/exp4_cache_effect.csv"

  run_bench "$MAIN_ROOT" exp8
  require_lines "$MAIN_ROOT/raw/exp8_range_query.csv" 145

  run_bench "$MAIN_ROOT" exp9
  require_lines "$MAIN_ROOT/raw/exp9_range_filter_strategy.csv" 57
  require_exp9_selected_strategy "$MAIN_ROOT/raw/exp9_range_filter_strategy.csv"

  run_bench "$MAIN_ROOT" exp5
  require_lines "$MAIN_ROOT/raw/exp5_index_comparison.csv" 769

  run_bench "$MAIN_ROOT" exp6
  require_lines "$MAIN_ROOT/raw/exp6_iterative_scan.csv" 36

  run_bench "$MAIN_ROOT" exp7
  require_lines "$MAIN_ROOT/raw/exp7_half_precision.csv" 13

  run_bench "$MAIN_ROOT" exp10,exp11
  require_phase_f_perf "$MAIN_ROOT/raw/exp_f_chain_perf.csv"
  require_phase_f_invalidation "$MAIN_ROOT/raw/exp_f_chain_invalidation.csv"

  run_analysis "$MAIN_ROOT"
}

run_audit_full() {
  run_bench "$AUDIT_ROOT" exp2
  require_lines "$AUDIT_ROOT/raw/exp2_selectivity_accuracy.csv" 57

  run_bench "$AUDIT_ROOT" exp1
  require_lines "$AUDIT_ROOT/raw/exp1_strategy_comparison.csv" 385

  run_bench "$AUDIT_ROOT" exp3,exp4
  require_lines "$AUDIT_ROOT/raw/exp3_ssb_convergence.csv" 241
  require_file "$AUDIT_ROOT/raw/exp4_cache_effect.csv"

  run_bench "$AUDIT_ROOT" exp8
  require_lines "$AUDIT_ROOT/raw/exp8_range_query.csv" 145

  run_bench "$AUDIT_ROOT" exp9
  require_lines "$AUDIT_ROOT/raw/exp9_range_filter_strategy.csv" 57
  require_exp9_selected_strategy "$AUDIT_ROOT/raw/exp9_range_filter_strategy.csv"

  run_bench "$AUDIT_ROOT" exp5
  require_lines "$AUDIT_ROOT/raw/exp5_index_comparison.csv" 769

  run_bench "$AUDIT_ROOT" exp6
  require_lines "$AUDIT_ROOT/raw/exp6_iterative_scan.csv" 36

  run_bench "$AUDIT_ROOT" exp7
  require_lines "$AUDIT_ROOT/raw/exp7_half_precision.csv" 13

  run_bench "$AUDIT_ROOT" exp10,exp11
  require_phase_f_perf "$AUDIT_ROOT/raw/exp_f_chain_perf.csv"
  require_phase_f_invalidation "$AUDIT_ROOT/raw/exp_f_chain_invalidation.csv"

  run_analysis "$AUDIT_ROOT"
}

main() {
  prepare_go_server_env
  build_bench_if_needed
  log "orchestrator start"
  log "main and audit runs are executed sequentially, never concurrently"
  run_main_full
  run_audit_full
  require_matching_exp3_shape "$MAIN_ROOT/raw/exp3_ssb_convergence.csv" "$AUDIT_ROOT/raw/exp3_ssb_convergence.csv"
  write_final_docs
  log "orchestrator completed successfully"
}

main "$@"
