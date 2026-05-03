#!/usr/bin/env python3
import argparse
import csv
import sys
from collections import Counter
from pathlib import Path


MIN_DEP_SURVIVAL = 0.833333
REQUIRED_PHASE_F_SESSIONS = {
    "linear_explore",
    "backtrack_heavy",
    "rebucket_tuning",
    "multi_tab_shared",
}
REQUIRED_PHASE_F_DELTAS = {"AddFilter", "RebucketOnly"}
REQUIRED_PHASE_F_PATHS = {"ancestor_reuse", "full_hit", "cold_miss"}
ALLOWED_PHASE_F_FRAGMENTS = {"cellgrid", "candidates", "knn_refs", "buckets"}


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def validate_exp3_shape(args: argparse.Namespace) -> None:
    main_len, main_shape = exp3_shape(args.main)
    audit_len, audit_shape = exp3_shape(args.audit)
    if main_len == audit_len and main_shape == audit_shape:
        return
    print("exp3 convergence shape mismatch", file=sys.stderr)
    print(f"main_rows={main_len} audit_rows={audit_len}", file=sys.stderr)
    print(f"main_only={sorted((main_shape - audit_shape).elements())[:10]}", file=sys.stderr)
    print(f"audit_only={sorted((audit_shape - main_shape).elements())[:10]}", file=sys.stderr)
    raise SystemExit(1)


def exp3_shape(path: Path) -> tuple[int, Counter[tuple[str, str, str]]]:
    rows = read_rows(path)
    keys = Counter((row["query_id"], row["strategy"], row["batch_idx"]) for row in rows)
    return len(rows), keys


def validate_exp9_selected_strategy(args: argparse.Namespace) -> None:
    rows = read_rows(args.path)
    auto_rows = [row for row in rows if row["strategy"] == "auto"]
    blanks = [row for row in auto_rows if not row["selected_strategy"].strip()]
    if auto_rows and not blanks:
        return
    print("exp9 auto selected_strategy validation failed", file=sys.stderr)
    raise SystemExit(1)


def validate_phase_f_perf(args: argparse.Namespace) -> None:
    rows = read_rows(args.path)
    missing = phase_f_perf_missing(rows)
    if not missing:
        return
    print("exp10 Phase F perf validation failed: " + "; ".join(missing), file=sys.stderr)
    raise SystemExit(1)


def phase_f_perf_missing(rows: list[dict[str, str]]) -> list[str]:
    sessions = {row["session_type"] for row in rows}
    deltas = {row["delta_kind"] for row in rows}
    paths = {row["lookup_path"] for row in rows}
    missing = missing_values("sessions", REQUIRED_PHASE_F_SESSIONS, sessions)
    missing += missing_values("delta_kind", REQUIRED_PHASE_F_DELTAS, deltas)
    missing += missing_values("lookup_path", REQUIRED_PHASE_F_PATHS, paths)
    missing += phase_f_fragment_metadata_errors(rows)
    if not any(row["l0_hit"].strip().lower() == "true" for row in rows):
        missing.append("l0_hit=true")
    return missing


def phase_f_fragment_metadata_errors(rows: list[dict[str, str]]) -> list[str]:
    errors = []
    for index, row in enumerate(rows, start=2):
        path = row["lookup_path"].strip()
        fragments = phase_f_fragments(row)
        unknown = sorted(fragments - ALLOWED_PHASE_F_FRAGMENTS)
        if unknown:
            errors.append(f"line {index} invalid reused_fragments={unknown}")
        if path == "full_hit" and "cellgrid" not in fragments:
            errors.append(f"line {index} full_hit missing cellgrid")
        if path == "cold_miss" and fragments:
            errors.append(f"line {index} cold_miss has reused_fragments")
        if path == "ancestor_reuse" and not fragments:
            errors.append(f"line {index} ancestor_reuse missing reused_fragments")
    return errors


def phase_f_fragments(row: dict[str, str]) -> set[str]:
    return {
        item.strip()
        for item in row["reused_fragments"].split(",")
        if item.strip()
    }


def missing_values(label: str, required: set[str], actual: set[str]) -> list[str]:
    diff = sorted(required - actual)
    return [f"{label}={diff}"] if diff else []


def validate_phase_f_invalidation(args: argparse.Namespace) -> None:
    rows = read_rows(args.path)
    require_no_missed_invalidations(rows)
    dep_rows = [row for row in rows if row["invalidation_type"] == "dependency"]
    all_rows = [row for row in rows if row["invalidation_type"] == "all"]
    require_rows(dep_rows, "exp11 dependency invalidation row missing")
    require_rows(all_rows, "exp11 invalidate_all row missing")
    require_dependency_survival(dep_rows)
    require_invalidate_all_survival(all_rows)


def require_no_missed_invalidations(rows: list[dict[str, str]]) -> None:
    if any(int(row["missed_invalidations"]) != 0 for row in rows):
        fail("exp11 missed invalidations must be zero")


def require_rows(rows: list[dict[str, str]], message: str) -> None:
    if not rows:
        fail(message)


def require_dependency_survival(rows: list[dict[str, str]]) -> None:
    if any(float(row["survival_rate"]) < MIN_DEP_SURVIVAL for row in rows):
        fail("exp11 dependency survival_rate below 0.833333")


def require_invalidate_all_survival(rows: list[dict[str, str]]) -> None:
    if any(float(row["survival_rate"]) != 0 for row in rows):
        fail("exp11 invalidate_all survival_rate must be 0")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    exp3 = sub.add_parser("exp3-shape")
    exp3.add_argument("main", type=Path)
    exp3.add_argument("audit", type=Path)
    exp3.set_defaults(func=validate_exp3_shape)
    one_path_command(sub, "exp9-selected-strategy", validate_exp9_selected_strategy)
    one_path_command(sub, "phase-f-perf", validate_phase_f_perf)
    one_path_command(sub, "phase-f-invalidation", validate_phase_f_invalidation)
    return parser


def one_path_command(sub: argparse._SubParsersAction, name: str, func) -> None:
    command = sub.add_parser(name)
    command.add_argument("path", type=Path)
    command.set_defaults(func=func)


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
