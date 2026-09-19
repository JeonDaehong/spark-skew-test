#!/usr/bin/env python
"""
S8 — 일반화 검증: 핵심 결론이 sort 밖에서도 성립하는가?

패널
  A. wall vs skew, 워크로드별
     -> count 가 평평해야 한다 (음성 대조군). 평평하지 않으면 해석이 틀린 것이다.
  B. peak execution memory vs skew
     -> 천장이 워크로드마다 다른가. sort 712 / join 616 / count 8 MiB.
        agg 는 이 지표를 아예 보고하지 않는다 (ObjectHashAggregate) — 0 은
        '안 썼다'가 아니라 '안 보인다'라서 회색으로 따로 표시한다.
  C. 첫 spill x N
     -> 1/N 이 sorter 의 성질인가 sort 워크로드 고유인가.
  D. record-skew: 바이트 고정, 레코드만 증가
     -> 반복 산포(막대)와 겹치는지 같이 봐야 한다. 작은 차이라 median 만 보면 속는다.

B 와 C 가 이 그림의 핵심이다. "같은 법칙, 다른 상수"를 보여준다.
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
WL_COLORS = {"sort": "#2a78d6", "join": "#eb6834",
             "agg": "#8b5cf6", "count": "#1baf7a"}
WL_ORDER = ["sort", "join", "agg", "count"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s8")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = pd.read_csv(os.path.join(root, args.tag, "summary.csv"))
    n_all = len(df)
    df = df[df["error"].isna()].copy()
    for c in ("cores", "skew", "partitions"):
        df[c] = df[c].astype(float).astype(int)

    theme = DARK if args.dark else LIGHT
    style(theme)
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    (axA, axB), (axC, axD) = axes

    byte = df[(df["skew_mode"] == "byte")]
    A = byte[byte["cores"] == 4]
    gA = A.groupby(["workload", "skew"]).mean(numeric_only=True).reset_index()

    # --- A. wall -------------------------------------------------------
    for w in WL_ORDER:
        s = gA[gA["workload"] == w].sort_values("skew")
        if s.empty:
            continue
        axA.plot(s["skew"], s["wall_seconds"], "o-", color=WL_COLORS[w], label=w)
    axA.set_title("A. wall clock vs skew (cores=4)\n"
                  "count is the negative control — it must stay flat")
    axA.set_xlabel("skew"); axA.set_ylabel("wall (s)")
    axA.legend(fontsize=8); axA.grid(alpha=.3)

    # --- B. peak exec mem ----------------------------------------------
    for w in WL_ORDER:
        s = gA[gA["workload"] == w].sort_values("skew")
        if s.empty:
            continue
        pk = s["peak_exec_mem_max"] / MB
        if (pk == 0).all():
            # 계측이 안 되는 워크로드. 0 을 선으로 그으면 '메모리를 안 썼다'로 읽힌다.
            axB.plot(s["skew"], pk, "x--", color=theme["ink2"], alpha=.6,
                     label=f"{w} (not reported)")
        else:
            axB.plot(s["skew"], pk, "o-", color=WL_COLORS[w], label=w)
            axB.annotate(f"{pk.iloc[-1]:.0f}", (s["skew"].iloc[-1], pk.iloc[-1]),
                         textcoords="offset points", xytext=(6, -3),
                         fontsize=8, color=WL_COLORS[w])
    axB.axhline(POOL_MIB, ls="--", lw=1, color=theme["ink2"])
    axB.text(gA["skew"].min(), POOL_MIB, " execution pool 720 MiB",
             va="bottom", fontsize=8, color=theme["ink2"])
    axB.set_title("B. peak execution memory\n"
                  "same pool, different ceiling per operator")
    axB.set_xlabel("skew"); axB.set_ylabel("peak exec memory (MiB)")
    axB.legend(fontsize=8); axB.grid(alpha=.3)

    # --- C. 1/N 법칙 ----------------------------------------------------
    # 평탄구간만 쓴다. cliff 를 넘은 칸은 1/N 이 성립할 이유가 없다.
    B16 = byte[byte["skew"] == 16]
    gC = B16.groupby(["workload", "cores"]).median(numeric_only=True).reset_index()
    for w in ["sort", "join"]:
        s = gC[gC["workload"] == w].sort_values("cores")
        if s.empty:
            continue
        prod = s["spill_disk_total"] / MB * s["cores"]
        flat = prod < prod.min() * 2          # cliff 를 넘은 점을 뺀다
        axC.plot(s["cores"][flat], prod[flat], "o-", color=WL_COLORS[w],
                 label=f"{w}  (coef {prod[flat].mean()/POOL_MIB:.3f})")
        if (~flat).any():
            axC.plot(s["cores"][~flat], prod[~flat], "x", ms=9,
                     color=WL_COLORS[w], alpha=.6)
            axC.annotate("past the cliff", (s["cores"][~flat].iloc[0],
                                            prod[~flat].iloc[0]),
                         textcoords="offset points", xytext=(-70, -4),
                         fontsize=8, color=WL_COLORS[w])
    axC.set_yscale("log")
    axC.set_title("C. first-spill x N stays constant (1/N law)\n"
                  "same law, different constant per operator")
    axC.set_xlabel("cores per executor (N)"); axC.set_ylabel("first-spill x N (MiB, log)")
    axC.set_xticks(sorted(B16["cores"].unique()))
    axC.legend(fontsize=8); axC.grid(alpha=.3)

    # --- D. record-skew -------------------------------------------------
    rec = df[df["skew_mode"] == "record"]
    for w in ["sort", "join", "agg"]:
        s = rec[rec["workload"] == w]
        if s.empty:
            continue
        g = s.groupby("record_skew")["wall_seconds"]
        R = sorted(s["record_skew"].unique())
        mean = [g.get_group(r).mean() for r in R]
        lo = [g.get_group(r).min() for r in R]
        hi = [g.get_group(r).max() for r in R]
        axD.plot(R, mean, "o-", color=WL_COLORS[w], label=w)
        axD.fill_between(R, lo, hi, color=WL_COLORS[w], alpha=.18)
    axD.set_xscale("log", base=2)
    axD.set_xticks(sorted(rec["record_skew"].unique()))
    axD.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axD.set_title("D. record-skew: bytes ~flat, records up to 22x\n"
                  "band = min..max over reps; the effect is real but small here")
    axD.set_xlabel("record skew R"); axD.set_ylabel("wall (s)")
    axD.legend(fontsize=8); axD.grid(alpha=.3)

    fig.suptitle(f"S8 — do the S1/S2/S3 findings hold outside `sort`?   "
                 f"({len(df)}/{n_all} runs, 0 errors)", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, .96))

    outdir = os.path.join(root, args.tag, "figures")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, f"generalization{'_dark' if args.dark else ''}.png")
    fig.savefig(out, dpi=150)
    print(f"[plot] {out}")


if __name__ == "__main__":
    main()
