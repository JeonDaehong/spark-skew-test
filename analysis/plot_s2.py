#!/usr/bin/env python
"""
S2 — 계단의 위치가 row width 에 따라 움직이는가?  (명제 P2)

S1 은 "계단이 있다"를 보였다. S2 는 "그 계단을 누가 놓았는가"를 묻는다.
총 바이트를 고정한 채 row width 만 바꾸면 record 수가 반비례로 변한다.

  bytes 가 범인  → 계단 위치는 row width 와 무관 (세 곡선이 겹침)
  records 가 범인 → row 가 좁을수록 계단이 왼쪽으로 이동

rb=256 은 S1 결과를 재사용한다 (다른 조건이 전부 동일).

패널 (이중 축 금지):
  A. 바이트당 비용 (ms/MB)            — 계단이 어디에 있나
  B. peak execution memory + 천장선   — 언제 풀이 포화되나
  C. hot 파티션 record 수 (백만)       — record 축에서 보면 어떻게 정렬되나
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style
from plot_normalized import pool_ceiling_mb

MB = 2 ** 20


def load(root, tag):
    p = os.path.join(root, tag, "summary.csv")
    if not os.path.exists(p):
        return None
    df = pd.read_csv(p)
    if "error" in df:
        df = df[df["error"].isna()]
    df["src"] = tag
    return df


def prep(df):
    df = df.copy()
    df["hot_mb"] = df["sr_bytes_max"] / MB
    df["ms_per_mb"] = df["task_ms_max"] / df["hot_mb"]
    df["peak_mb"] = df["peak_exec_mem_max"] / MB
    df["hot_mrec"] = df["sr_records_max"] / 1e6
    return df


def step_location(g, ceiling):
    """실행 풀이 포화되는 첫 skew (peak >= 천장의 95%). 계단의 메커니즘상 위치."""
    peak = g.groupby("skew")["peak_mb"].median()
    hit = peak[peak >= ceiling * 0.95]
    return float(hit.index.min()) if len(hit) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=None)
    ap.add_argument("--tags", default="s2,s1")
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")

    parts = [d for d in (load(root, t) for t in args.tags.split(",")) if d is not None]
    if not parts:
        raise SystemExit("no summary.csv found")
    df = prep(pd.concat(parts, ignore_index=True))

    # S2 가 돈 skew 레벨로 맞춘다 (S1 의 노이즈 구간 1,2 는 제외)
    levels = sorted(df[df["src"] == "s2"]["skew"].unique()) if (df["src"] == "s2").any() \
        else sorted(df["skew"].unique())
    df = df[df["skew"].isin(levels)]

    widths = sorted(df["row_bytes"].unique())
    theme = DARK if args.dark else LIGHT
    style(theme)
    colors = dict(zip(widths, theme["series"]))
    ceiling = pool_ceiling_mb(df["exec_mem"].iloc[0])

    panels = [
        ("ms_per_mb", "A.  cost per hot-partition MB  (ms / MB)", ceiling if False else None),
        ("peak_mb",   "B.  peak execution memory  (MB)", ceiling),
        ("hot_mrec",  "C.  hot partition records  (millions)", None),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    for ax, (col, title, hline) in zip(axes, panels):
        for rb in widths:
            g = df[df["row_bytes"] == rb]
            c = colors[rb]
            a = g.groupby("skew")[col].median()
            q1 = g.groupby("skew")[col].quantile(0.25)
            q3 = g.groupby("skew")[col].quantile(0.75)
            ax.fill_between(a.index, q1, q3, color=c, alpha=0.13, linewidth=0)
            ax.plot(a.index, a.values, color=c, marker="o", markersize=5,
                    markeredgecolor=theme["surface"], markeredgewidth=1.4,
                    label=f"{rb} B/row", zorder=3)
            ax.scatter(g["skew"], g[col], color=c, s=9, alpha=0.28,
                       linewidths=0, zorder=2)
        if hline:
            ax.axhline(hline, color=theme["ink2"], linestyle=(0, (5, 3)), linewidth=1.4)
            ax.annotate(f"execution pool ceiling  {hline:.0f} MB",
                        (min(levels), hline), xytext=(0, 6), textcoords="offset points",
                        color=theme["ink2"], fontsize=9, fontweight="bold")
        ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
        ax.set_xscale("log", base=2)
        ax.set_xticks(levels)
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_xlabel("skew degree")
        ax.margins(x=0.08)
        ax.set_ylim(bottom=0)

    axes[0].legend(frameon=False, loc="upper left", labelcolor=theme["ink2"],
                   title="row width", title_fontproperties={"size": 9})
    fig.suptitle("Is the step set by bytes, or by records?",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    primary = args.tags.split(",")[0]
    out = os.path.join(root, primary, "figures",
                       f"row_width{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")

    # ---- 숫자 판정 ----
    print("\n=== 바이트당 비용 (ms/MB), row width 별 median ===")
    piv = df.pivot_table(index="skew", columns="row_bytes",
                         values="ms_per_mb", aggfunc="median")
    print(piv.round(2).to_string())

    print("\n=== peak execution memory (MB) ===")
    print(df.pivot_table(index="skew", columns="row_bytes",
                         values="peak_mb", aggfunc="median").round(0).to_string())

    print(f"\n=== 계단 위치 (실행 풀 >= 천장 {ceiling:.0f}MB 의 95%) ===")
    for rb in widths:
        loc = step_location(df[df["row_bytes"] == rb], ceiling)
        print(f"  row_bytes={rb:>5}  →  skew {loc if loc else '미도달'}")

    print("\n판정:")
    print("  세 곡선의 계단이 같은 skew 에 있으면  → bytes 가 천장을 정한다 (P2 거짓)")
    print("  좁은 row 일수록 계단이 왼쪽이면       → records 가 기여한다 (P2 참)")


if __name__ == "__main__":
    main()
