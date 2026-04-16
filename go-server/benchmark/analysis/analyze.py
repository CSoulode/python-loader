import sys

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.colors import ListedColormap

from helpers import load_csv, save_current, save_figure
from phase_f_plots import analyze_exp10, analyze_exp11
from range_plots import analyze_exp8, analyze_exp9


sns.set_theme(style="whitegrid", context="talk")

BEST_STRATEGY_ORDER = ["post_filter", "pre_filter", "hybrid"]
BEST_STRATEGY_COLORS = ["#4C78A8", "#F58518", "#54A24B"]
EXP5_SELECTIVITY_MARKERS = ["o", "s", "D", "^", "v", "P", "X", "*"]


def dataset_style(df: pd.DataFrame) -> str:
    return "dataset_label" if "dataset_label" in df.columns else "dataset_size"


def flatten_axes(axes) -> list:
    return list(axes.flat) if hasattr(axes, "flat") else [axes]


def format_selectivity_labels(values) -> list[str]:
    return [f"{float(value):.3f}" for value in values]


def exp5_marker_map(df: pd.DataFrame) -> dict[str, str]:
    labels = sorted(format_selectivity_labels(df["selectivity"].unique()))
    return {label: EXP5_SELECTIVITY_MARKERS[index % len(EXP5_SELECTIVITY_MARKERS)] for index, label in enumerate(labels)}


def legacy_knn_subset(df: pd.DataFrame) -> pd.DataFrame:
    if "query_mode" not in df.columns:
        return df.copy()
    knn = df[df["query_mode"] == "knn"].copy()
    return knn if not knn.empty else df.copy()


def build_best_strategy_table(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ranked = df[df["strategy"] != "auto"].sort_values("ttlb_ms")
    best = ranked.groupby(["k", "selectivity"], as_index=False).first()
    label_table = best.pivot_table(index="k", columns="selectivity", values="strategy", aggfunc="first")
    label_table.columns = format_selectivity_labels(label_table.columns)
    encoded = label_table.apply(lambda col: col.map(BEST_STRATEGY_ORDER.index))
    return label_table, encoded


def shorten_strategy(value: str) -> str:
    return {"post_filter": "post", "pre_filter": "pre", "hybrid": "hyb"}.get(value, value)


def analyze_exp1() -> None:
    df = load_csv("exp1_strategy_comparison.csv")
    if df is None or df.empty:
        return
    plot_df = legacy_knn_subset(df)
    label_table, encoded = build_best_strategy_table(plot_df)
    annotations = label_table.apply(lambda col: col.map(shorten_strategy))
    plt.figure(figsize=(8, 4.5))
    sns.heatmap(encoded, annot=annotations, fmt="", cmap=ListedColormap(BEST_STRATEGY_COLORS), cbar=False, linewidths=0.5, linecolor="white")
    plt.title("Experiment 1 Best Strategy Heatmap")
    plt.xlabel("Actual Selectivity")
    plt.ylabel("k")
    save_current("exp1_strategy_heatmap")

    fixed_k = plot_df[plot_df["k"] == 500]
    plt.figure(figsize=(8, 4.5))
    sns.lineplot(data=fixed_k, x="selectivity", y="ttlb_ms", hue="strategy", style=dataset_style(fixed_k), estimator="median", marker="o")
    plt.title("Experiment 1 TTLB vs Actual Selectivity")
    plt.xlabel("Actual Selectivity")
    save_current("exp1_ttlb_vs_selectivity")


def analyze_exp2() -> None:
    df = load_csv("exp2_selectivity_accuracy.csv")
    if df is None or df.empty:
        return
    plt.figure(figsize=(6, 6))
    sns.scatterplot(data=df, x="actual", y="estimated", hue="complexity", style=dataset_style(df))
    limit = max(df["actual"].max(), df["estimated"].max())
    plt.plot([0, limit], [0, limit], linestyle="--", color="black", linewidth=1)
    plt.title("Experiment 2 Estimated vs Actual")
    save_current("exp2_estimation_scatter")

    error_df = df.copy()
    error_df["bias"] = (error_df["ratio"] - 1.0).abs()
    plt.figure(figsize=(7, 4.5))
    sns.barplot(data=error_df, x="complexity", y="bias", hue=dataset_style(error_df), estimator="mean")
    plt.title("Experiment 2 Error by Complexity")
    save_current("exp2_error_by_complexity")


def analyze_exp3() -> None:
    df = load_csv("exp3_ssb_convergence.csv")
    if df is None or df.empty:
        return
    plt.figure(figsize=(8, 4.5))
    sns.lineplot(data=df, x="elapsed_ms", y="jsd", hue="strategy", style=dataset_style(df), estimator="mean", marker="o")
    plt.title("Experiment 3 JSD Convergence")
    save_current("exp3_jsd_convergence")


def analyze_exp4() -> None:
    df = load_csv("exp4_cache_effect.csv")
    if df is None or df.empty:
        return
    plt.figure(figsize=(8, 4.5))
    sns.barplot(data=df, x="query_id", y="total_10_rebucket_ms", hue="mode")
    plt.title("Experiment 4 Cache Speedup")
    save_current("exp4_cache_speedup")


def analyze_exp5() -> None:
    df = load_csv("exp5_index_comparison.csv")
    if df is None or df.empty:
        return
    plot_df = legacy_knn_subset(df)
    plot_df["selectivity_label"] = format_selectivity_labels(plot_df["selectivity"])
    grid = sns.relplot(data=plot_df, x="recall_at_k", y="ttlb_ms", hue="index_type", style="selectivity_label", markers=exp5_marker_map(plot_df), col="model", kind="scatter", height=4.8, aspect=0.95)
    grid.set_axis_labels("Recall@K", "TTLB (ms)")
    grid.set_titles("{col_name}")
    grid.fig.suptitle("Experiment 5 Recall vs Latency by Model", y=1.02)
    if grid._legend is not None:
        grid.fig.subplots_adjust(right=0.82)
        grid._legend.set_bbox_to_anchor((1.02, 0.5))
        grid._legend._loc = 6
    save_figure(grid.fig, "exp5_pareto_recall_latency")

    plt.figure(figsize=(7, 4.5))
    sns.barplot(data=df.drop_duplicates(["dataset_label", "dataset_size", "model", "index_type"]), x="index_type", y="index_size_mb", hue="model")
    plt.title("Experiment 5 Index Sizes")
    save_current("exp5_index_sizes")

    order = ["hnsw", "ivfflat", "diskann", "no_index", "none"]
    figure, axes = plt.subplots(1, plot_df["model"].nunique(), figsize=(14, 4.8), sharey=True)
    for axis, (model, model_df) in zip(flatten_axes(axes), plot_df.groupby("model", sort=True)):
        summary = summarize_best_index(model_df, order)
        heat = summary.pivot_table(index="k", columns="selectivity", values="label", aggfunc="first")
        heat.columns = format_selectivity_labels(heat.columns)
        encoded = heat.apply(lambda col: col.map(lambda value: order.index(value) if value in order else -1))
        sns.heatmap(encoded, annot=heat, fmt="", cmap="viridis", ax=axis, cbar=False, annot_kws={"size": 11})
        axis.set_title(model)
        axis.set_xlabel("Actual Selectivity")
        axis.set_yticklabels(axis.get_yticklabels(), rotation=0)
    flatten_axes(axes)[0].set_ylabel("K")
    figure.suptitle("Experiment 5 Best Index by Workload", y=1.02)
    save_figure(figure, "exp5_optimal_index_heatmap")


def analyze_exp6() -> None:
    df = load_csv("exp6_iterative_scan.csv")
    if df is None or df.empty:
        return
    plot_df = df.copy()
    plot_df["return_rate"] = plot_df["returned_count"] / plot_df["k"]
    ann_flag = "plan_used_vector_ann_index" if "plan_used_vector_ann_index" in plot_df.columns else "plan_used_index"
    plot_df[ann_flag] = plot_df[ann_flag].astype(str).str.lower() == "true"

    figure_a, axes_a = plt.subplots(1, plot_df["index_type"].nunique(), figsize=(14, 4.8), sharey=True)
    for axis, (index_type, subset) in zip(flatten_axes(axes_a), plot_df.groupby("index_type", sort=True)):
        sns.lineplot(data=subset, x="selectivity", y="return_rate", hue="iterative_scan", marker="o", ax=axis)
        axis.set_title(index_type)
        axis.set_xlabel("Actual Selectivity")
    flatten_axes(axes_a)[0].set_ylabel("Returned / K")
    figure_a.suptitle("Experiment 6 Return Rate", y=1.02)
    save_figure(figure_a, "exp6_return_rate")

    figure_b, axes_b = plt.subplots(1, plot_df["index_type"].nunique(), figsize=(14, 4.8), sharey=True)
    for axis, (index_type, subset) in zip(flatten_axes(axes_b), plot_df.groupby("index_type", sort=True)):
        sns.lineplot(data=subset, x="selectivity", y="recall_at_k", hue="iterative_scan", marker="o", ax=axis)
        axis.set_title(index_type)
        axis.set_xlabel("Actual Selectivity")
    flatten_axes(axes_b)[0].set_ylabel("Recall@K")
    figure_b.suptitle("Experiment 6 Recall vs Actual Selectivity", y=1.02)
    save_figure(figure_b, "exp6_recall_vs_selectivity")

    figure_c, axes_c = plt.subplots(1, plot_df["index_type"].nunique(), figsize=(14, 4.8), sharey=True)
    for axis, (index_type, subset) in zip(flatten_axes(axes_c), plot_df.groupby("index_type", sort=True)):
        sns.lineplot(data=subset, x="selectivity", y=ann_flag, hue="iterative_scan", marker="o", ax=axis)
        axis.set_title(index_type)
        axis.set_xlabel("Actual Selectivity")
    flatten_axes(axes_c)[0].set_ylabel("ANN Index Used")
    figure_c.suptitle("Experiment 6 ANN Index Usage", y=1.02)
    save_figure(figure_c, "exp6_vector_ann_usage")


def analyze_exp7() -> None:
    df = load_csv("exp7_half_precision.csv")
    if df is None or df.empty:
        return
    figure, axis = plt.subplots(figsize=(7.5, 4.8))
    sns.barplot(data=df.drop_duplicates(["dataset_label", "dataset_size", "index_type", "precision"]), x="index_type", y="index_size_mb", hue="precision", ax=axis)
    axis.set_title("Experiment 7 Size Comparison")
    if df[(df["index_type"] == "diskann") & (df["precision"] == "half")].empty:
        axis.text(0.98, 0.98, "DiskANN half unsupported on pilot host", transform=axis.transAxes, ha="right", va="top", fontsize=10)
    save_figure(figure, "exp7_size_comparison")

    pivot = df.pivot_table(index=["dataset_label", "dataset_size", "index_type", "k"], columns="precision", values="recall_at_k", aggfunc="median").reset_index()
    if {"full", "half"}.issubset(pivot.columns):
        pivot = pivot.dropna(subset=["full", "half"]).copy()
        if pivot.empty:
            return
        pivot["recall_delta"] = (pivot["full"] - pivot["half"]).abs()
        figure, axis = plt.subplots(figsize=(7.5, 4.8))
        sns.barplot(data=pivot, x="k", y="recall_delta", hue="index_type", ax=axis)
        axis.set_title("Experiment 7 Recall Delta")
        if df[(df["index_type"] == "diskann") & (df["precision"] == "half")].empty:
            axis.text(0.98, 0.98, "Only HNSW has full/half pair on pilot host", transform=axis.transAxes, ha="right", va="top", fontsize=10)
        save_figure(figure, "exp7_recall_delta")


def summarize_best_index(df: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    winners = []
    for (k, selectivity), group in df.groupby(["k", "selectivity"], sort=True):
        qualified = group[group["recall_at_k"] >= 0.95]
        if qualified.empty:
            winners.append({"k": k, "selectivity": selectivity, "label": "none"})
            continue
        best = qualified.sort_values("ttlb_ms").iloc[0]
        winners.append({"k": k, "selectivity": selectivity, "label": best["index_type"]})
    return pd.DataFrame(winners)


def main() -> None:
    analyzers = {
        "exp1": analyze_exp1,
        "exp2": analyze_exp2,
        "exp3": analyze_exp3,
        "exp4": analyze_exp4,
        "exp5": analyze_exp5,
        "exp6": analyze_exp6,
        "exp7": analyze_exp7,
        "exp8": analyze_exp8,
        "exp9": analyze_exp9,
        "exp10": analyze_exp10,
        "exp11": analyze_exp11,
    }
    selected = sys.argv[1:] or list(analyzers.keys())
    for name in selected:
        analyzer = analyzers.get(name)
        if analyzer is None:
            raise SystemExit(f"unsupported analysis target: {name}")
        analyzer()


if __name__ == "__main__":
    main()
