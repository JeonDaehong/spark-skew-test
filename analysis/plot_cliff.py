#!/usr/bin/env python
"""
S1 산출 그림.

  cliff.png       skew vs task duration — 이 프로젝트의 전제를 판정하는 그림
  mechanism.png   cliff 지점에서 어떤 메트릭이 같이 움직이는가 (small multiples)
  skew_check.png  목표 skew 가 실제로 구현됐는가 (B1 검증)

그림 규칙
---------
- 이중 축 금지. 스케일이 다른 측정치는 facet 으로 분리한다.
- 계열은 고정 순서 3색(blue/orange/aqua)까지만. 그 이상은 facet 으로 쪼갠다.
- 반복 측정은 점으로 전부 찍고, 선은 median, 띠는 IQR.
  cliff 을 주장하려면 꺾임이 IQR 보다 커야 하므로 산포를 숨기면 안 된다.
- aqua 는 밝은 배경에서 대비가 3:1 미만이라 반드시 직접 라벨을 같이 단다.
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LIGHT = dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", grid="#e4e3df",
             series=("#2a78d6", "#eb6834", "#1baf7a"))
DARK = dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", grid="#333331",
            series=("#3987e5", "#d95926", "#199e70"))


def style(theme):
    plt.rcParams.update({
        "figure.facecolor": theme["surface"],
        "axes.facecolor": theme["surface"],
        "savefig.facecolor": theme["surface"],
        "text.color": theme["ink"],
        "axes.labelcolor": theme["ink2"],
        "axes.edgecolor": theme["grid"],
        "xtick.color": theme["ink2"],
        "ytick.color": theme["ink2"],
        "grid.color": theme["grid"],
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.grid": True,
        "grid.linewidth": 0.7,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "lines.linewidth": 2.0,
        "figure.dpi": 130,
    })


def agg(df, col):
    """skew 별 median / IQR."""
    g = df.groupby("skew")[col]
    return pd.DataFrame({
        "skew": sorted(df["skew"].unique()),
        "med": g.median().values,
        "q1": g.quantile(0.25).values,
        "q3": g.quantile(0.75).values,
    })


def pool_ceiling_mb(exec_mem):
    """spark.memory.fraction 기반 실행 메모리 풀 상한 추정 (MB)."""
    t = str(exec_mem).strip().lower()
    mb = float(t[:-1]) * 1024 if t.endswith("g") else float(t.rstrip("m"))
    return (mb - 300) * 0.6


def spill_onset(df):
    """
    cliff 마커의 위치.

    'spill > 0' 은 쓰면 안 된다 — S1 에서 skew 4/8/16 이 전부 동일하게 136.5MB 를
    spill 했다. 즉 skew 와 무관한 고정 배경 spill 이 존재한다. 그걸 onset 으로
    찍으면 잘못된 지점을 가리킨다.

    대신 '실행 메모리 풀이 포화된 지점'(peak >= 천장의 95%)을 쓴다. 이게 메커니즘상
    의미 있는 경계이고, S1 에서 바이트당 비용이 계단식으로 뛴 지점과 일치한다.
    """
    if "peak_exec_mem_max" not in df or "exec_mem" not in df:
        return None
    ceiling = pool_ceiling_mb(df["exec_mem"].iloc[0])
    peak_mb = df.groupby("skew")["peak_exec_mem_max"].median() / 2 ** 20
    hit = peak_mb[peak_mb >= ceiling * 0.95]
    return float(hit.index.min()) if len(hit) else None


def fig_cliff(df, theme, out):
    series = [("task_ms_p50", "p50", theme["series"][0]),
              ("task_ms_p90", "p90", theme["series"][1]),
              ("task_ms_max", "max", theme["series"][2])]

    fig, ax = plt.subplots(figsize=(8.4, 5.2))
    ends = []
    for col, label, c in series:
        if col not in df:
            continue
        a = agg(df, col)
        ax.fill_between(a["skew"], a["q1"], a["q3"], color=c, alpha=0.13, linewidth=0)
        ax.plot(a["skew"], a["med"], color=c, label=label,
                marker="o", markersize=5, markeredgecolor=theme["surface"],
                markeredgewidth=1.5, zorder=3)
        # 반복 측정 원자료 — 산포를 숨기지 않는다
        ax.scatter(df["skew"], df[col], color=c, s=9, alpha=0.30,
                   linewidths=0, zorder=2)
        ends.append([a["skew"].iloc[-1], a["med"].iloc[-1], label, c])

    # 직접 라벨 (aqua 대비 보완 겸용). 값이 붙어 있으면 세로로 벌려 겹침을 막는다.
    span = (max(e[1] for e in ends) - min(e[1] for e in ends)) or 1.0
    ends.sort(key=lambda e: e[1])
    for i in range(1, len(ends)):
        gap = ends[i][1] - ends[i - 1][1]
        if gap < span * 0.07:
            ends[i][1] = ends[i - 1][1] + span * 0.07
    for x, y, label, c in ends:
        ax.annotate(label, (x, y), xytext=(8, 0), textcoords="offset points",
                    color=c, fontsize=10, fontweight="bold", va="center")

    onset = spill_onset(df)
    if onset:
        ax.axvline(onset, color=theme["ink2"], linestyle=(0, (4, 3)),
                   linewidth=1.2, zorder=1)
        ax.annotate(f"execution pool saturated  skew={onset:g}", (onset, ax.get_ylim()[1]),
                    xytext=(6, -12), textcoords="offset points",
                    color=theme["ink2"], fontsize=9)

    ax.set_xscale("log", base=2)
    ax.set_xticks(sorted(df["skew"].unique()))
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel("skew degree  (hot partition / median partition)")
    ax.set_ylabel("reduce task duration  (ms)")
    ax.set_title("Where does task latency break as skew grows?",
                 color=theme["ink"], pad=14, loc="left")
    ax.margins(x=0.08)
    ax.grid(axis="x", alpha=0.35)
    leg = ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])
    leg.set_title("task duration quantile", prop={"size": 9})
    leg.get_title().set_color(theme["ink2"])
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


PANELS = [
    ("spill_disk_total", "spill to disk (MB)", 1 / 2**20),
    ("peak_exec_mem_max", "peak execution memory (MB)", 1 / 2**20),
    ("gc_ms_total", "JVM GC (ms)", 1),
    ("delta_psi_io_full_us", "PSI io.full (s)", 1e-6),
    ("peak_dirty_kb", "peak page-cache Dirty (MB)", 1 / 1024),
    ("wall_seconds", "wall clock (s)", 1),
]


def fig_mechanism(df, theme, out):
    panels = [(c, lbl, sc) for c, lbl, sc in PANELS if c in df.columns]
    n = len(panels)
    ncol = 3
    nrow = -(-n // ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(11.5, 3.1 * nrow), sharex=True)
    axes = np.atleast_1d(axes).ravel()
    onset = spill_onset(df)

    for ax, (col, label, scale) in zip(axes, panels):
        d = df.copy()
        d[col] = d[col] * scale
        a = agg(d, col)
        c = theme["series"][0]
        ax.fill_between(a["skew"], a["q1"], a["q3"], color=c, alpha=0.15, linewidth=0)
        ax.plot(a["skew"], a["med"], color=c, marker="o", markersize=4,
                markeredgecolor=theme["surface"], markeredgewidth=1.2)
        ax.scatter(d["skew"], d[col], color=c, s=8, alpha=0.28, linewidths=0)
        if onset:
            ax.axvline(onset, color=theme["ink2"], linestyle=(0, (4, 3)), linewidth=1.0)
        ax.set_title(label, loc="left", fontsize=10.5, color=theme["ink"])
        ax.set_xscale("log", base=2)
        ax.set_xticks(sorted(df["skew"].unique()))
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.margins(x=0.08)

    for ax in axes[n:]:
        ax.set_visible(False)
    for ax in axes[max(0, n - ncol):n]:
        ax.set_xlabel("skew degree")

    fig.suptitle("What moves together at the cliff?",
                 x=0.01, ha="left", color=theme["ink"], fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


def fig_skew_check(df, theme, out):
    """생성기가 의도한 skew 를 실제로 만들었는가. 이게 어긋나면 나머지가 전부 무의미."""
    fig, ax = plt.subplots(figsize=(5.6, 5.2))
    lim = [0, max(df["skew"].max(), df["actual_size_skew"].max()) * 1.08]
    ax.plot(lim, lim, color=theme["ink2"], linestyle=(0, (4, 3)), linewidth=1.2,
            label="target = actual")
    ax.scatter(df["skew"], df["actual_size_skew"], color=theme["series"][0],
               s=42, alpha=0.75, linewidths=1.2, edgecolors=theme["surface"],
               label="actual (shuffle read bytes)", zorder=3)
    ax.set_xlabel("target skew degree")
    ax.set_ylabel("actual skew (max / median shuffle read bytes)")
    ax.set_title("Generator validation (B1)", loc="left", color=theme["ink"], pad=12)
    ax.set_xlim(lim)
    ax.set_ylim(lim)
    ax.set_aspect("equal")
    ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s1")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.join(args.results or os.path.join(repo, "results"), args.tag)
    df = pd.read_csv(os.path.join(root, "summary.csv"))
    df = df[df["error"].isna()] if "error" in df else df

    theme = DARK if args.dark else LIGHT
    style(theme)
    suffix = "_dark" if args.dark else ""
    outdir = os.path.join(root, "figures")
    os.makedirs(outdir, exist_ok=True)

    made = [
        fig_cliff(df, theme, os.path.join(outdir, f"cliff{suffix}.png")),
        fig_mechanism(df, theme, os.path.join(outdir, f"mechanism{suffix}.png")),
        fig_skew_check(df, theme, os.path.join(outdir, f"skew_check{suffix}.png")),
    ]
    for m in made:
        print(f"[plot] {m}")

    # cliff 판정을 눈이 아니라 숫자로도 남긴다.
    a = agg(df, "task_ms_max")
    a["slope"] = a["med"].diff() / a["skew"].diff()
    a["iqr"] = a["q3"] - a["q1"]
    print("\n[cliff 판정용] task_ms_max")
    print(a.to_string(index=False))
    print("\n기울기가 가장 크게 변하는 구간이 cliff 후보. "
          "그 구간의 med 변화량이 iqr 보다 커야 주장할 수 있다.")


if __name__ == "__main__":
    main()
