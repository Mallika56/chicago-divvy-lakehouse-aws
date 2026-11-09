"""Turn the query results in docs/results/ into the README charts (docs/img/)."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RES, IMG = ROOT / "docs/results", ROOT / "docs/img"
IMG.mkdir(parents=True, exist_ok=True)

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e0"
COLOR = {"member": "#2a78d6", "casual": "#eb6834"}
MONTHS = "Oct Nov Dec Jan Feb Mar Apr May Jun Jul Aug Sep".split()

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
})


def hourly() -> None:
    """Plot weekday vs. weekend ride-share-by-hour for members and casual riders."""
    df = pd.read_csv(RES / "03_hourly_profile.csv")
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), sharey=True)
    for ax, day in zip(axes, ["weekday", "weekend"]):
        for rider in ["member", "casual"]:
            d = df[(df.day_type == day) & (df.member_casual == rider)].sort_values("start_hour")
            share = 100 * d.rides / d.rides.sum()       # % of that group's rides, so shapes compare
            ax.plot(d.start_hour, share, color=COLOR[rider], lw=2, label=rider)
            if day == "weekday":   # weekend lines end together; the legend covers them
                ax.annotate(rider, (d.start_hour.iloc[-1], share.iloc[-1]), xytext=(4, 0),
                            textcoords="offset points", va="center", color=INK2, fontsize=9)
        ax.set_title(day.capitalize(), loc="left", fontsize=11)
        ax.set_xticks([0, 6, 12, 18, 23], ["12am", "6am", "12pm", "6pm", "11pm"])
        ax.set_xlim(0, 26)
    axes[0].set_ylabel("% of the group's rides")
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Weekdays: members have a sharp 8am commute peak, casual riders don't. Weekends look alike.",
                 x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMG / "hourly_profile.png", dpi=160)


def monthly() -> None:
    """Plot monthly ride counts by rider type across the sampled year."""
    df = pd.read_csv(RES / "04_monthly_trend.csv")
    fig, ax = plt.subplots(figsize=(10, 3.6))
    x = range(12)
    for i, rider in enumerate(["member", "casual"]):
        d = df[df.member_casual == rider].reset_index(drop=True)
        ax.bar([p + (i - 0.5) * 0.4 for p in x], d.rides, width=0.38, color=COLOR[rider], label=rider)
    ax.set_xticks(list(x), [f"{m}\n{'2025' if j < 3 else '2026'}" if j in (0, 3) else m
                            for j, m in enumerate(MONTHS)])
    ax.set_ylabel("Rides in the 1/256 sample")
    ax.grid(axis="x", visible=False)
    ax.legend(frameon=False)
    ax.set_title("Ridership is seasonal, and the casual share drops in winter",
                 loc="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMG / "monthly_trend.png", dpi=160)


def stations() -> None:
    """Plot the top 10 stations for casual-rider starts."""
    df = pd.read_csv(RES / "05_top_casual_stations.csv").iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.barh(df.start_station_name, df.casual_rides, color=COLOR["casual"], height=0.6)
    for y, (v, pct) in enumerate(zip(df.casual_rides, df.pct_casual)):
        ax.text(v + 2, y, f"{v}  ({pct:.0f}% casual)", va="center", color=INK2, fontsize=9)
    ax.set_xlabel("Casual-rider starts in the 1/256 sample")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, df.casual_rides.max() * 1.3)
    ax.set_title("Top 10 stations for casual riders: lakefront and museums",
                 loc="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMG / "top_casual_stations.png", dpi=160)


if __name__ == "__main__":
    hourly()
    monthly()
    stations()
    print("charts written to", IMG)
