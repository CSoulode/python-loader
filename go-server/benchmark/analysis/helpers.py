from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "raw"
FIG_DIR = ROOT / "figures"


def load_csv(name: str) -> pd.DataFrame | None:
    path = RAW_DIR / name
    if not path.exists():
        return None
    return pd.read_csv(path)


def ensure_fig_dir() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)


def save_current(name: str) -> None:
    save_figure(plt.gcf(), name)


def save_figure(fig: plt.Figure, name: str) -> None:
    ensure_fig_dir()
    pdf_path = FIG_DIR / f"{name}.pdf"
    png_path = FIG_DIR / f"{name}.png"
    fig.tight_layout()
    fig.savefig(pdf_path, bbox_inches="tight")
    fig.savefig(png_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
