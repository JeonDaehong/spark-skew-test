#!/usr/bin/env python
"""
정규화 그림 — 이 프로젝트에서 가장 중요한 분석.

raw 곡선(cliff.png)은 "hot 파티션이 커졌으니 오래 걸린다"만 보여준다. 그건 당연하다.
진짜 질문은:

    hot 파티션의 **바이트당 비용**이 skew 에 따라 변하는가?

변하지 않으면 skew 는 그냥 "데이터가 몰린 것"이고 새로울 게 없다.
변한다면, 어디서 왜 변하는지가 이 연구의 대상이다.

패널 구성 (이중 축 금지 — 각 측정치는 자기 축을 가진 별도 패널):
  A. max task duration / hot partition MB   = 바이트당 비용
  B. peak execution memory + 실행 풀 상한선  = Spark 메모리 관리자가 천장에 닿는 지점
  C. spill to disk                           = 천장에 닿은 결과
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style, agg

MB = 2 ** 20


def pool_ceiling_mb(exec_mem: str) -> float:
    """spark.memory.fraction 기반 실행 메모리 풀 상한 추정."""
    s = str(exec_mem).strip().lower()
    mb = float(s[:-1]) * 1024 if s.endswith("g") else float(s.rstrip("m"))
    return (mb - 300) * 0.6


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s1")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.join(args.results or os.path.join(repo, "results"), args.tag)
    df = pd.read_csv(os.path.join(root, "summary.csv"))
    if "error" in df:
        df = df[df["error"].isna()]

    theme = DARK if args.dark else LIGHT
    style(theme)
    c = theme["series"][0]
    accent = theme["series"][1]

    # 바이트당 비용
    df = df.copy()
    df["hot_mb"] = df["sr_bytes_max"] / MB
    df["ms_per_mb"] = df["task_ms_max"] / df["hot_mb"]
    df["peak_mb"] = df["peak_exec_mem_max"] / MB
    df["spill_mb"] = df["spill_disk_total"] / MB

    ceiling = pool_ceiling_mb(df["exec_mem"].iloc[0])

    panels = [
        ("ms_per_mb", "A.  cost per hot-partition MB  (ms / MB)", None),
        ("peak_mb",   "B.  peak execution memory  (MB)", ceiling),
        ("spill_mb",  "C.  spill to disk  (MB)", None),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.3))
    for ax, (col, title, hline) in zip(axes, panels):
        a = agg(df, col)
        ax.fill_between(a["skew"], a["q1"], a["q3"], color=c, alpha=0.15, linewidth=0)
        ax.plot(a["skew"], a["med"], color=c, marker="o", markersize=5,
                markeredgecolor=theme["surface"], markeredgewidth=1.4, zorder=3)
        ax.scatter(df["skew"], df[col], color=c, s=10, alpha=0.30, linewidths=0, zorder=2)
        if hline:
            ax.axhline(hline, color=accent, linestyle=(0, (5, 3)), linewidth=1.6, zorder=1)
            ax.annotate(f"execution pool ceiling  {hline:.0f} MB",
                        (a["skew"].iloc[0], hline), xytext=(0, 6),
                        textcoords="offset points", color=accent, fontsize=9,
                        fontweight="bold")
        if col == "ms_per_mb":
            # skew <= 2 에서는 hot 파티션이 아직 지배적이지 않아 max task 가
            # 데이터가 아니라 straggler 다. 산포(IQR)가 값만큼 크다. 숨기지 않고 표시한다.
            noise = df[df["skew"] <= 2]["skew"]
            if len(noise):
                ax.axvspan(df["skew"].min() * 0.85, 2 * 1.4,
                           color=theme["ink2"], alpha=0.07, zorder=0)
                ax.annotate("noise-dominated\n(max task = straggler,\n not the hot partition)",
                            (1.15, ax.get_ylim()[1] * 0.62),
                            color=theme["ink2"], fontsize=8.5, ha="left", va="top")
        ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
        ax.set_xscale("log", base=2)
        ax.set_xticks(sorted(df["skew"].unique()))
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_xlabel("skew degree")
        ax.margins(x=0.08)
        ax.set_ylim(bottom=0)

    fig.suptitle("Does a skewed partition get more expensive per byte?",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    out = os.path.join(root, "figures",
                       f"normalized{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")

    # 숫자로도 남긴다
    med = df.groupby("skew")[["hot_mb", "ms_per_mb", "peak_mb", "spill_mb"]].median()
    iqr = (df.groupby("skew")["ms_per_mb"].quantile(0.75)
           - df.groupby("skew")["ms_per_mb"].quantile(0.25))
    med["ms_per_mb_iqr"] = iqr
    med["vs_min"] = (med["ms_per_mb"] / med["ms_per_mb"].min() - 1) * 100
    print("\n=== 바이트당 비용 ===")
    print(med.round(2).to_string())
    print("\nvs_min = 최저점 대비 증가율(%). "
          "이 증가폭이 ms_per_mb_iqr 보다 충분히 크면 '바이트당 비용이 변했다'고 말할 수 있다.")


if __name__ == "__main__":
    main()
