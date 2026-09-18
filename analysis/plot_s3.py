#!/usr/bin/env python
"""
S3 — executor core 수가 cliff 를 앞당기는가?  (명제 P4)

흔한 튜닝 조언: "executor 당 core 를 늘리면 태스크마다 메모리 풀의 1/N 만
쓸 수 있으니 더 일찍 spill 한다."

S1 에서 우리는 그 조언의 전제를 이미 의심했다. 혼자 도는 태스크는 풀 전체를
가져갔다. 1/2N~1/N 은 **하한**이지 상한이 아니다. 그래서 P4 의 예측은 갈린다.

패널
  A. peak execution memory vs skew, core 수별
     -> 조언이 맞다면 천장이 core 수에 따라 내려가야 한다.
  B. spill 총량 vs skew (log), core 수별
     -> 계단의 위치(cliff)가 core 수에 따라 움직이는가.
  C. 첫 spill 크기 x N
     -> 평탄구간 spill 이 정확히 1/N 로 줄어드는지.
  D. core 스케일링 효율 (1 core 대비 speedup)
     -> skew 가 병렬 확장성을 얼마나 깎아먹는가.

A 와 B 가 서로 다른 답을 낸다는 것이 이 그림의 요점이다.
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style

MB = 2 ** 20
POOL_MIB = 720.0  # (1500m - 300m) * 0.6
# cores 는 4 수준이라 plot_cliff 의 3색 팔레트로는 모자라다.
CORE_COLORS = {1: "#2a78d6", 2: "#1baf7a", 4: "#eb6834", 6: "#8b5cf6"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s3")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = pd.read_csv(os.path.join(root, args.tag, "summary.csv"))

    # OOM 으로 죽은 run 은 지표가 0 으로 남는다. 평균에 섞으면 안 된다.
    n_all = len(df)
    oom = df[df["error"].notna()]
    df = df[df["error"].isna()].copy()
    for c in ("cores", "skew"):
        df[c] = df[c].astype(float).astype(int)

    theme = DARK if args.dark else LIGHT
    style(theme)
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    (axA, axB), (axC, axD) = axes

    cores = sorted(df["cores"].unique())
    g = df.groupby(["cores", "skew"]).mean(numeric_only=True).reset_index()

    # --- A. peak execution memory -------------------------------------
    for c in cores:
        s = g[g["cores"] == c].sort_values("skew")
        axA.plot(s["skew"], s["peak_exec_mem_max"] / MB, "o-",
                 color=CORE_COLORS[c], label=f"{c} core")
    axA.axhline(POOL_MIB, ls="--", lw=1, color=theme["ink2"])
    axA.text(df["skew"].max(), POOL_MIB, " execution pool 720 MiB",
             va="bottom", ha="right", fontsize=8, color=theme["ink2"])
    # 조언이 예측하는 천장들
    for c in cores[1:]:
        axA.axhline(POOL_MIB / c, ls=":", lw=1, color=CORE_COLORS[c], alpha=.7)
        axA.text(df["skew"].min(), POOL_MIB / c, f" pool/{c}",
                 va="bottom", fontsize=7, color=CORE_COLORS[c], alpha=.9)
    axA.set_title("A. hot task peak execution memory\n"
                  "(dotted = ceiling the '1/N share' advice predicts)")
    axA.set_xlabel("skew"); axA.set_ylabel("peak exec memory (MiB)")
    axA.legend(fontsize=8); axA.grid(alpha=.3)

    # --- B. spill 총량 ------------------------------------------------
    for c in cores:
        s = g[g["cores"] == c].sort_values("skew")
        axB.plot(s["skew"], s["spill_disk_total"] / MB, "o-",
                 color=CORE_COLORS[c], label=f"{c} core")
    axB.set_yscale("symlog", linthresh=10)
    axB.set_title("B. total spill to disk\n"
                  "(cliff = jump from flat plateau to data-proportional)")
    axB.set_xlabel("skew"); axB.set_ylabel("spill (MiB, symlog)")
    axB.legend(fontsize=8); axB.grid(alpha=.3)

    # --- C. 첫 spill 의 1/N 법칙 ---------------------------------------
    # 평탄구간 = 각 core 수에서 가장 낮은 skew (아직 cliff 전)
    lo = g["skew"].min()
    base = g[(g["skew"] == lo)].set_index("cores")["spill_disk_total"] / MB
    ns = [c for c in cores if base.get(c, 0) > 0]
    axC.plot(ns, [base[c] * c for c in ns], "o-", color=theme["series"][0],
             label="measured  first-spill x N")
    axC.plot(ns, [base[ns[0]] * ns[0]] * len(ns), "--", lw=1,
             color=theme["ink2"], label="perfect 1/N")
    for c in ns:
        axC.annotate(f"{c}c: {base[c]:.1f} MiB", (c, base[c] * c),
                     textcoords="offset points", xytext=(6, -12), fontsize=8)
    axC.set_ylim(0, max(base[c] * c for c in ns) * 1.35)
    axC.set_title("C. first-spill size scales as 1/N\n"
                  f"(flat regime, skew={lo}; product stays ~constant)")
    axC.set_xlabel("cores per executor (N)")
    axC.set_ylabel("first-spill x N (MiB)")
    axC.set_xticks(ns); axC.legend(fontsize=8); axC.grid(alpha=.3)

    # --- D. core 스케일링 효율 ------------------------------------------
    base1 = g[g["cores"] == 1].set_index("skew")["wall_seconds"]
    for sk in sorted(g["skew"].unique()):
        s = g[g["skew"] == sk].sort_values("cores")
        axD.plot(s["cores"], base1[sk] / s["wall_seconds"], "o-",
                 label=f"skew {sk}", alpha=.9)
    axD.plot(cores, cores, "--", lw=1, color=theme["ink2"], label="ideal")
    axD.set_title("D. skew eats your core scaling\n"
                  "(speedup vs 1 core, same data, same pool)")
    axD.set_xlabel("cores per executor"); axD.set_ylabel("speedup")
    axD.set_xticks(cores); axD.legend(fontsize=7, ncol=2); axD.grid(alpha=.3)

    note = f"{len(df)}/{n_all} runs"
    if len(oom):
        note += "  |  " + ", ".join(
            f"OOM c{int(float(r.cores))} skew{int(float(r.skew))}"
            for r in oom.itertuples())
    fig.suptitle(f"S3 — does more cores per executor move the cliff?   ({note})",
                 fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, .96))

    outdir = os.path.join(root, args.tag, "figures")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, f"cores{'_dark' if args.dark else ''}.png")
    fig.savefig(out, dpi=150)
    print(f"[plot] {out}")


if __name__ == "__main__":
    main()
