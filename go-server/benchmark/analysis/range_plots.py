import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.colors import ListedColormap

from helpers import load_csv, save_current, save_figure


EXP8_INDEX_ORDER = ["hnsw", "ivfflat", "diskann", "no_index"]
EXP8_INDEX_COLORS = ["#4C78A8", "#F58518", "#54A24B", "#9D755D"]
EXP8_PATH_SHORT = {"knn_adapter": "adapter", "brute_force": "brute"}
EXP9_STRATEGY_ORDER = ["post_filter", "pre_filter", "hybrid"]
EXP9_STRATEGY_COLORS = ["#4C78A8", "#F58518", "#54A24B"]


def _encode_categories(table: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    return table.apply(lambda col: col.map(lambda value: order.index(value) if value in order else -1))


def _format_selectivity_labels(values) -> list[str]:
    return [f"{float(value):.3f}" for value in values]


def _exp8_frame() -> pd.DataFrame | None:
    df = load_csv("exp8_range_query.csv")
    if df is None or df.empty:
        return None
    frame = df.copy()
    if "range_position" not in frame.columns:
        frame["range_position"] = frame["query_type"]
    if "range_width_label" not in frame.columns:
        frame["range_width_label"] = frame["range_width"].map(lambda value: f"{float(value):.3f}")
    return frame


def analyze_exp8() -> None:
    df = _exp8_frame()
    if df is None:
        return

    scatter = sns.relplot(
        data=df.sort_values(["model", "query_type", "range_width"]),
        x="result_count",
        y="latency_ms",
        hue="query_type",
        style="index_type",
        col="model",
        kind="scatter",
        height=4.8,
        aspect=0.95,
    )
    scatter.set_axis_labels("Result Count", "Latency (ms)")
    scatter.set_titles("{col_name}")
    scatter.fig.suptitle("Experiment 8 Result Count vs Latency", y=1.03)
    save_figure(scatter.fig, "exp8_result_vs_latency")

    best = df.sort_values("latency_ms").groupby(["model", "query_type", "range_width_label", "range_position"], as_index=False).first()
    panels = best[["model", "query_type"]].drop_duplicates().sort_values(["model", "query_type"])
    figure, axes = plt.subplots(1, len(panels), figsize=(6.2 * len(panels), 4.8), squeeze=False)
    for axis, (_, panel) in zip(axes.flat, panels.iterrows()):
        subset = best[(best["model"] == panel["model"]) & (best["query_type"] == panel["query_type"])]
        labels = subset.pivot_table(index="range_width_label", columns="range_position", values="index_type", aggfunc="first")
        encoded = _encode_categories(labels, EXP8_INDEX_ORDER)
        annotations = subset.pivot_table(index="range_width_label", columns="range_position", values="strategy", aggfunc="first")
        annotations = annotations.apply(lambda col: col.map(lambda value: EXP8_PATH_SHORT.get(value, value)))
        sns.heatmap(
            encoded,
            annot=annotations.fillna(""),
            fmt="",
            cmap=ListedColormap(EXP8_INDEX_COLORS),
            cbar=False,
            linewidths=0.5,
            linecolor="white",
            ax=axis,
        )
        axis.set_title(f"{panel['model']} / {panel['query_type']}")
        axis.set_xlabel("Range Position")
        axis.set_ylabel("Range Width Label")
    figure.suptitle("Experiment 8 Best Index / Path by Range Shape", y=1.03)
    save_figure(figure, "exp8_optimal_index_heatmap")

    line = sns.relplot(
        data=df.sort_values("range_width"),
        x="range_width",
        y="latency_ms",
        hue="query_type",
        style="index_type",
        col="model",
        kind="line",
        marker="o",
        height=4.8,
        aspect=0.95,
    )
    line.set_axis_labels("Range Width", "Latency (ms)")
    line.set_titles("{col_name}")
    line.fig.suptitle("Experiment 8 Ball vs Ring Latency", y=1.03)
    save_figure(line.fig, "exp8_ball_vs_ring")


def _exp9_frame() -> pd.DataFrame | None:
    df = load_csv("exp9_range_filter_strategy.csv")
    if df is None or df.empty:
        return None
    frame = df.copy()
    if "query_type" not in frame.columns and "range_type" in frame.columns:
        frame["query_type"] = frame["range_type"]
    if "selected_strategy" not in frame.columns:
        frame["selected_strategy"] = frame["strategy"]
    return frame


def analyze_exp9() -> None:
    df = _exp9_frame()
    if df is None:
        return

    grid = sns.relplot(
        data=df.sort_values("selectivity"),
        x="selectivity",
        y="ttlb_ms",
        hue="strategy",
        col="query_type",
        kind="line",
        marker="o",
        height=4.5,
        aspect=1.05,
    )
    grid.set_axis_labels("Actual Selectivity", "TTLB (ms)")
    grid.set_titles("{col_name}")
    grid.fig.suptitle("Experiment 9 TTLB vs Selectivity", y=1.03)
    save_figure(grid.fig, "exp9_ttlb_vs_selectivity")

    auto_rows = df[df["strategy"] == "auto"].copy()
    if auto_rows.empty:
        auto_rows = df[df["strategy"] != "auto"].sort_values("ttlb_ms").groupby(["query_type", "selectivity"], as_index=False).first()
        auto_rows["selected_strategy"] = auto_rows["strategy"]
    labels = auto_rows.pivot_table(index="query_type", columns="selectivity", values="selected_strategy", aggfunc="first")
    labels.columns = _format_selectivity_labels(labels.columns)
    encoded = _encode_categories(labels, EXP9_STRATEGY_ORDER)
    plt.figure(figsize=(8, 3.8))
    sns.heatmap(
        encoded,
        annot=labels.fillna(""),
        fmt="",
        cmap=ListedColormap(EXP9_STRATEGY_COLORS),
        cbar=False,
        linewidths=0.5,
        linecolor="white",
    )
    plt.title("Experiment 9 Auto-Selected Strategy Heatmap")
    plt.xlabel("Actual Selectivity")
    plt.ylabel("Query Type")
    save_current("exp9_strategy_heatmap")
