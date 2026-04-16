import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from helpers import load_csv, save_current


def analyze_exp10() -> None:
    df = load_csv("exp10_bs_cache_performance.csv")
    if df is None or df.empty:
        return

    plot_df = df.copy()
    plot_df["request_idx"] = plot_df["request_idx"].astype(int)
    plot_df["l0_hit"] = plot_df["l0_hit"].astype(str).str.lower() == "true"
    plot_df["l1_hit"] = plot_df["l1_hit"].astype(str).str.lower() == "true"
    plot_df["path"] = plot_df.apply(exp10_path_label, axis=1)

    cumulative = build_exp10_cumulative_hits(plot_df)
    plt.figure(figsize=(10, 5.2))
    sns.lineplot(
        data=cumulative,
        x="request_idx",
        y="cumulative_hit_rate",
        hue="session_type",
        style="layer",
        marker="o",
    )
    plt.ylim(0, 1.05)
    plt.title("Experiment 10 L0/L1 Hit Rate")
    plt.ylabel("Cumulative Hit Rate")
    save_current("exp10_l0_l1_hit_rate")

    latency_df = plot_df[plot_df["path"].isin(["l0_hit", "l1_hit", "l1_miss"])].copy()
    plt.figure(figsize=(10, 5.2))
    sns.barplot(
        data=latency_df,
        x="session_type",
        y="end_to_end_ms",
        hue="path",
        estimator="mean",
    )
    plt.title("Experiment 10 Latency by Cache Path")
    plt.ylabel("End-to-End Latency (ms)")
    save_current("exp10_latency_by_path")

    scatter_df = plot_df[plot_df["path"] != "l0_hit"].copy()
    plt.figure(figsize=(8.2, 5.2))
    sns.scatterplot(
        data=scatter_df,
        x="l1_cache_entries",
        y="end_to_end_ms",
        hue="path",
        style="session_type",
    )
    plt.title("Experiment 10 L1 Entries vs Latency")
    plt.ylabel("End-to-End Latency (ms)")
    save_current("exp10_l1_entries_vs_latency")


def build_exp10_cumulative_hits(df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for session_type, subset in df.groupby("session_type", sort=True):
        ordered = subset.sort_values("request_idx").copy()
        ordered["l0_rate"] = ordered["l0_hit"].cumsum() / ordered["request_idx"]
        ordered["l1_rate"] = ordered["l1_hit"].cumsum() / ordered["request_idx"]
        frames.append(
            pd.DataFrame(
                {
                    "session_type": session_type,
                    "request_idx": ordered["request_idx"],
                    "layer": "l0_hit",
                    "cumulative_hit_rate": ordered["l0_rate"],
                }
            )
        )
        frames.append(
            pd.DataFrame(
                {
                    "session_type": session_type,
                    "request_idx": ordered["request_idx"],
                    "layer": "l1_hit",
                    "cumulative_hit_rate": ordered["l1_rate"],
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def exp10_path_label(row: pd.Series) -> str:
    if row["l0_hit"]:
        return "l0_hit"
    if row["l1_hit"]:
        return "l1_hit"
    return "l1_miss"


def analyze_exp11() -> None:
    df = load_csv("exp11_conditional_requests.csv")
    if df is None or df.empty:
        return

    plot_df = df.copy()
    plot_df["not_modified_count"] = plot_df["not_modified_count"].astype(int)
    plot_df["fresh_response_count"] = plot_df["fresh_response_count"].astype(int)
    plot_df["entries_before"] = pd.to_numeric(plot_df["entries_before"], errors="coerce")
    plot_df["survival_rate"] = pd.to_numeric(plot_df["survival_rate"], errors="coerce")

    figure, axis = plt.subplots(figsize=(9.2, 5.2))
    axis.bar(
        plot_df["scenario"],
        plot_df["not_modified_count"],
        label="not_modified",
    )
    axis.bar(
        plot_df["scenario"],
        plot_df["fresh_response_count"],
        bottom=plot_df["not_modified_count"],
        label="fresh_response",
    )
    axis.set_title("Experiment 11 Not-Modified Ratio")
    axis.set_ylabel("Request Count")
    axis.legend()
    save_current("exp11_not_modified_ratio")

    survival_df = plot_df[plot_df["survival_rate"].notna()].copy()
    if survival_df.empty:
        return
    plt.figure(figsize=(7.8, 5.2))
    sns.barplot(data=survival_df, x="scenario", y="survival_rate")
    plt.ylim(0, 1.05)
    plt.title("Experiment 11 Survival Rate")
    plt.ylabel("Survival Rate")
    save_current("exp11_survival_rate")
