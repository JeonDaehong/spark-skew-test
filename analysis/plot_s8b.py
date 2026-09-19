#!/usr/bin/env python
"""
S8b — 키별 연산에서 계단은 언제 생기고 언제 안 생기는가?

S8 은 집계 팔을 `collect_list(substring(payload,1,8))` 로 잡는 바람에 shuffle 이
32 배 줄어 풀 근처도 못 갔다. 여기서 그 구멍을 메운다.

패널
  A. wall vs skew — window 가 sort 를 따라가는가
  B. peak execution memory — 같은 천장(712 MiB)에 닿는가.
     이 패널이 핵심이다. 천장이 같으면 계단은 sort 고유가 아니라
     "키별로 큰 파티션을 메모리에 쌓는 연산" 일반의 성질이다.
  C. spill — 안전밸브가 실제로 열리는가
  D. agg_wide 의 운명. **실패가 결과인 팔이다.**
     base 는 spill 0 인 채로 죽고, fb0 은 spill 하고도 죽는다는 것을 보인다.
     성공/실패를 색이 아니라 마커로 구분한다 (색맹 안전).
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style

MB = 2 ** 20
POOL_MIB = 720.0
COLORS = {"window": "#2a78d6", "sort": "#eb6834",
          "base": "#8b5cf6", "fb0": "#1baf7a"}


def agg(df, key, metric):
    """워크로드/라벨별 skew 평균. 실패 run 은 제외한다 (지표가 0 으로 남는다)."""
    ok = df[df["error"].isna()]
    g = ok[ok["_arm"] == key].groupby("skew")[metric].mean()
    return g.sort_index()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s8b")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = pd.read_csv(os.path.join(root, args.tag, "summary.csv"))
    df["skew"] = df["skew"].astype(float).astype(int)
    df["label"] = df["label"].fillna("")
    # 팔 이름: window / sort / base / fb0
    df["_arm"] = df.apply(
        lambda r: r["label"] if r["workload"] == "agg_wide"
        else ("sort" if r["workload"] == "sort" else r["workload"]), axis=1)

    n_all, n_err = len(df), int(df["error"].notna().sum())

    theme = DARK if args.dark else LIGHT
    style(theme)
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    (axA, axB), (axC, axD) = axes

    # --- A. wall --------------------------------------------------------
    for arm in ["window", "sort"]:
        s = agg(df, arm, "wall_seconds")
        axA.plot(s.index, s.values, "o-", color=COLORS[arm], label=arm)
    axA.set_title("A. wall clock\nwindow tracks sort")
    axA.set_xlabel("skew"); axA.set_ylabel("wall (s)")
    axA.legend(fontsize=8); axA.grid(alpha=.3)

    # --- B. peak exec mem (핵심) ----------------------------------------
    for arm in ["window", "sort"]:
        s = agg(df, arm, "peak_exec_mem_max") / MB
        axB.plot(s.index, s.values, "o-", color=COLORS[arm], label=arm)
        axB.annotate(f"{s.iloc[-1]:.1f}", (s.index[-1], s.iloc[-1]),
                     textcoords="offset points", xytext=(6, -3),
                     fontsize=9, color=COLORS[arm])
    axB.axhline(POOL_MIB, ls="--", lw=1, color=theme["ink2"])
    axB.text(df["skew"].min(), POOL_MIB, " execution pool 720 MiB",
             va="bottom", fontsize=8, color=theme["ink2"])
    axB.set_title("B. peak execution memory\nsame ceiling = same mechanism")
    axB.set_xlabel("skew"); axB.set_ylabel("peak exec memory (MiB)")
    axB.legend(fontsize=8); axB.grid(alpha=.3)

    # --- C. spill -------------------------------------------------------
    for arm in ["window", "sort"]:
        s = agg(df, arm, "spill_disk_total") / MB
        axC.plot(s.index, s.values, "o-", color=COLORS[arm], label=arm)
    axC.set_title("C. spill to disk\nthe safety valve opens")
    axC.set_xlabel("skew"); axC.set_ylabel("spill (MiB)")
    axC.legend(fontsize=8); axC.grid(alpha=.3)

    # --- D. agg_wide 의 운명 ---------------------------------------------
    skews = sorted(df["skew"].unique())
    width = 0.35
    for i, arm in enumerate(["base", "fb0"]):
        sub = df[df["_arm"] == arm]
        if sub.empty:
            continue
        xs, spills, died = [], [], []
        for j, sk in enumerate(skews):
            rs = sub[sub["skew"] == sk]
            if rs.empty:
                continue
            xs.append(j + (i - 0.5) * width)
            spills.append(rs["spill_disk_total"].mean() / MB)
            died.append(bool(rs["error"].notna().any()))
        axD.bar(xs, spills, width, color=COLORS[arm], alpha=.85,
                label=f"agg_wide ({arm})")
        top = max(spills) if spills else 1
        for x, sp, d in zip(xs, spills, died):
            axD.annotate("OOM" if d else "ok", (x, sp),
                         textcoords="offset points", xytext=(0, 4),
                         ha="center", fontsize=8,
                         color=theme["ink"] if d else theme["ink2"],
                         fontweight="bold" if d else "normal")
    axD.set_xticks(range(len(skews)))
    axD.set_xticklabels(skews)
    axD.set_title("D. agg_wide — collect_list(payload)\n"
                  "failure is the result: no valve (base) vs valve but still dies (fb0)")
    axD.set_xlabel("skew"); axD.set_ylabel("spill (MiB)")
    axD.legend(fontsize=8); axD.grid(alpha=.3, axis="y")

    fig.suptitle("S8b — where the cliff appears, and where the safety valve is missing   "
                 f"({n_all} runs, {n_err} failed by design)", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, .96))

    outdir = os.path.join(root, args.tag, "figures")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, f"per_key_ops{'_dark' if args.dark else ''}.png")
    fig.savefig(out, dpi=150)
    print(f"[plot] {out}")


if __name__ == "__main__":
    main()
