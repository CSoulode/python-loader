import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from helpers import load_csv, save_current


EXP10_FILES = ["exp_f_chain_perf.csv", "exp10_bs_cache_performance.csv"]
EXP11_FILES = ["exp_f_chain_invalidation.csv", "exp11_conditional_requests.csv"]


def load_first(names: list[str]) -> pd.DataFrame | None:
    for name in names:
        frame = load_csv(name)
        if frame is not None and not frame.empty:
            return frame.copy()
    return None


def bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype(str).str.lower() == "true"


def normalize_exp10(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["request_idx"] = pd.to_numeric(out["request_idx"], errors="coerce")
    out["l0_hit"] = bool_series(out, "l0_hit")
    if "l1_hit" in out.columns:
        out["l1_hit"] = bool_series(out, "l1_hit")
    out["chain_hit"] = exp10_chain_hit(out)
    out["path"] = exp10_path(out)
    out["chain_nodes"] = exp10_node_count(out)
    return out


def exp10_chain_hit(frame: pd.DataFrame) -> pd.Series:
    if "l1_hit" in frame.columns:
        return bool_series(frame, "l1_hit")
    return frame["lookup_path"].isin(["ancestor_reuse", "full_hit"])


def exp10_path(frame: pd.DataFrame) -> pd.Series:
    if "lookup_path" in frame.columns:
        return frame["lookup_path"].replace({"": "cold_miss"})
    return frame.apply(exp10_legacy_path_label, axis=1)


def exp10_node_count(frame: pd.DataFrame) -> pd.Series:
    if "chain_nodes" in frame.columns:
        return pd.to_numeric(frame["chain_nodes"], errors="coerce")
    if "l1_cache_entries" in frame.columns:
        return pd.to_numeric(frame["l1_cache_entries"], errors="coerce")
    return pd.Series(0, index=frame.index)


def exp10_legacy_path_label(row: pd.Series) -> str:
    if bool(row["l0_hit"]):
        return "l0_hit"
    if bool(row["l1_hit"]):
        return "l1_hit"
    return "l1_miss"


def analyze_exp10() -> None:
    frame = load_first(EXP10_FILES)
    if frame is None:
        return
    plot_df = normalize_exp10(frame)
    plot_exp10_hit_rate(plot_df)
    plot_exp10_latency(plot_df)
    plot_exp10_nodes(plot_df)


def plot_exp10_hit_rate(frame: pd.DataFrame) -> None:
    cumulative = build_exp10_cumulative_hits(frame)
    plt.figure(figsize=(10, 5.2))
    sns.lineplot(data=cumulative, x="request_idx", y="cumulative_hit_rate", hue="session_type", style="layer", marker="o")
    plt.ylim(0, 1.05)
    plt.title("Experiment 10 L0 / State Chain Hit Rate")
    plt.ylabel("Cumulative Hit Rate")
    save_current("exp10_l0_chain_hit_rate")


def plot_exp10_latency(frame: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 5.2))
    sns.barplot(data=frame, x="session_type", y="end_to_end_ms", hue="path", estimator="mean")
    plt.title("Experiment 10 Latency by Cache Path")
    plt.ylabel("End-to-End Latency (ms)")
    save_current("exp10_latency_by_path")


def plot_exp10_nodes(frame: pd.DataFrame) -> None:
    scatter_df = frame[~frame["l0_hit"]].copy()
    plt.figure(figsize=(8.2, 5.2))
    sns.scatterplot(data=scatter_df, x="chain_nodes", y="end_to_end_ms", hue="path", style="session_type")
    plt.title("Experiment 10 Chain Nodes vs Latency")
    plt.ylabel("End-to-End Latency (ms)")
    save_current("exp10_chain_nodes_vs_latency")


def build_exp10_cumulative_hits(frame: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for session_type, subset in frame.groupby("session_type", sort=True):
        ordered = subset.sort_values("request_idx").copy()
        denominator = range(1, len(ordered) + 1)
        frames.append(exp10_rate_frame(session_type, ordered, denominator, "l0_hit"))
        frames.append(exp10_rate_frame(session_type, ordered, denominator, "chain_hit"))
    return pd.concat(frames, ignore_index=True)


def exp10_rate_frame(session_type: str, frame: pd.DataFrame, denominator: range, layer: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "session_type": session_type,
            "request_idx": frame["request_idx"],
            "layer": layer,
            "cumulative_hit_rate": frame[layer].cumsum() / list(denominator),
        }
    )


def analyze_exp11() -> None:
    frame = load_first(EXP11_FILES)
    if frame is None:
        return
    plot_df = normalize_exp11(frame)
    plot_exp11_response_mix(plot_df)
    plot_exp11_survival(plot_df)


def normalize_exp11(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ["not_modified_count", "fresh_response_count", "survival_rate"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    if "entries_before" in out.columns and "nodes_before" not in out.columns:
        out["nodes_before"] = pd.to_numeric(out["entries_before"], errors="coerce")
    return out


def plot_exp11_response_mix(frame: pd.DataFrame) -> None:
    figure, axis = plt.subplots(figsize=(9.2, 5.2))
    axis.bar(frame["scenario"], frame["not_modified_count"], label="not_modified")
    axis.bar(frame["scenario"], frame["fresh_response_count"], bottom=frame["not_modified_count"], label="fresh_response")
    axis.set_title("Experiment 11 Not-Modified Ratio")
    axis.set_ylabel("Request Count")
    axis.legend()
    save_current("exp11_not_modified_ratio")


def plot_exp11_survival(frame: pd.DataFrame) -> None:
    survival_df = frame[frame["survival_rate"].notna()].copy()
    if survival_df.empty:
        return
    plt.figure(figsize=(7.8, 5.2))
    sns.barplot(data=survival_df, x="scenario", y="survival_rate")
    plt.ylim(0, 1.05)
    plt.title("Experiment 11 Survival Rate")
    plt.ylabel("Survival Rate")
    save_current("exp11_survival_rate")
