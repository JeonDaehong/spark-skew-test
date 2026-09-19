#!/usr/bin/env python
"""
S11 / S11b — 실무 편: skew 를 어떻게 만나고 어떻게 막는가

블로그 앞부분("skew 는 어디서 생기고 무엇으로 막는가")을 측정으로 뒷받침하는 그림.
여섯 패널을 두 스윕에서 모은다.

  A. broadcast join 의 경계      (S11-A)  어디까지 빠르고 어디서 죽는가
  B. AQE 가 손대지 않는 구간     (S11-B)  hot < 256MB 면 켜도 안 쪼갠다
  C. AQE 임계값을 낮추면?        (S11b-D) 쪼개지긴 하는데 **시간은 안 준다**
  D. 해결책 4종 비교             (S11b-E) 무처리 / AQE / salting / hot key 분리
  E. 파티션 수 늘리기            (S11b-F) 음성 대조군 — 안 나아진다
  F. NULL key: inner vs outer    (S11b-G) Spark 가 알아서 해주는 경계

C 와 E 가 이 그림의 정직한 부분이다. 둘 다 "흔히 하는 처방이 안 듣는다"를 보인다.
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
ARM_COLORS = {"none": "#8a8a86", "aqe": "#2a78d6",
              "salt": "#eb6834", "isolate": "#1baf7a"}


def load(root, tag):
    df = pd.read_csv(os.path.join(root, tag, "summary.csv"))
    df["label"] = df["label"].fillna("")
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    a = load(root, "s11")
    b = load(root, "s11b")

    theme = DARK if args.dark else LIGHT
    style(theme)
    fig, axes = plt.subplots(2, 3, figsize=(17, 9.5))
    (axA, axB, axC), (axD, axE, axF) = axes

    # --- A. broadcast 경계 ---------------------------------------------
    bc = a[a["workload"] == "join_bc"]
    smj = a[(a["workload"] == "join") & (a["label"].str.startswith("smj"))]
    for sub, name, col in [(smj, "SortMergeJoin", "#8a8a86"),
                           (bc, "broadcast", "#2a78d6")]:
        gg = sub.groupby("dim_mb_est")
        xs, ys, died = [], [], []
        for mb, grp in gg:
            xs.append(mb)
            okg = grp[grp["error"].isna()]
            ys.append(okg["wall_seconds"].mean() if len(okg) else float("nan"))
            died.append(len(okg) == 0)
        axA.plot(xs, ys, "o-", color=col, label=name)
        for i, (x, y, d) in enumerate(zip(xs, ys, died)):
            if d:
                axA.plot(x, 0.6, "X", ms=13, color=col)
                axA.annotate("OOM", (x, 0.6), textcoords="offset points",
                             xytext=(0, 9 if i % 2 == 0 else 22), ha="center",
                             fontsize=8.5, fontweight="bold", color=col)
    axA.set_xscale("log")
    axA.set_ylim(bottom=0)
    axA.set_title("A. broadcast join: fast until it dies\n"
                  "(X = 'Not enough memory to build and broadcast')")
    axA.set_xlabel("dimension table size (MiB, log)"); axA.set_ylabel("wall (s)")
    axA.legend(fontsize=8); axA.grid(alpha=.3)

    # --- B. AQE 사각지대 -------------------------------------------------
    B = a[a["label"].isin(["aqefalse", "aqetrue"])]
    g = B.groupby(["skew", "label"]).mean(numeric_only=True).reset_index()
    sk = sorted(g["skew"].unique())
    # 주의: `g.skew` 는 컬럼이 아니라 DataFrame.skew() 메서드로 잡힌다 (pandas 3).
    # 조용히 전부 False 가 되어 빈 그래프가 나온다. 반드시 대괄호로 접근할 것.
    pick = lambda s, lb: g[(g["skew"] == s) & (g["label"] == lb)]
    off = [pick(s, "aqefalse")["sr_bytes_max"].mean() / MB for s in sk]
    on = [pick(s, "aqetrue")["sr_bytes_max"].mean() / MB for s in sk]
    axB.plot(sk, off, "o-", color="#8a8a86", label="AQE off")
    axB.plot(sk, on, "s--", color="#2a78d6", label="AQE on")
    axB.axhline(256, ls=":", lw=1.4, color="#eb6834")
    axB.text(sk[0], 256, " AQE threshold 256 MB", va="bottom",
             fontsize=8, color="#eb6834")
    axB.set_ylim(0, 300)
    axB.set_title("B. below the threshold AQE does nothing\n"
                  "(1 GiB data — even 50x skew stays under 256 MB)")
    axB.set_xlabel("skew"); axB.set_ylabel("hot partition (MiB)")
    axB.legend(fontsize=8); axB.grid(alpha=.3)

    # --- C. 임계값을 낮추면 ---------------------------------------------
    D = b[b["label"].str.startswith("aqe_")]
    order = ["aqe_off", "aqe_256m", "aqe_32m"]
    names = {"aqe_off": "AQE off", "aqe_256m": "AQE\n(256MB)", "aqe_32m": "AQE\n(32MB)"}
    sk2 = sorted(D["skew"].unique())
    w = 0.35
    for i, s in enumerate(sk2):
        vals = [D[(D["skew"] == s) & (D["label"] == o)]["wall_seconds"].mean()
                for o in order]
        xs = [j + (i - 0.5) * w for j in range(len(order))]
        axC.bar(xs, vals, w, label=f"skew {int(s)}",
                color=theme["series"][i % len(theme["series"])], alpha=.85)
    axC.set_xticks(range(len(order)))
    axC.set_xticklabels([names[o] for o in order], fontsize=9)
    axC.set_ylim(0, max(D["wall_seconds"]) * 1.25)
    axC.set_title("C. lowering the threshold splits — but buys nothing\n"
                  "(32MB makes AQE act; wall barely moves)")
    axC.set_ylabel("wall (s)"); axC.legend(fontsize=8); axC.grid(alpha=.3, axis="y")

    # --- D. 해결책 4종 ---------------------------------------------------
    E = b[b["label"].isin(["none", "aqe", "salt", "isolate"])]
    arms = ["none", "aqe", "salt", "isolate"]
    labels = {"none": "no fix", "aqe": "AQE", "salt": "salting\n(dim x16)",
              "isolate": "isolate\nhot key"}
    sk3 = sorted(E["skew"].unique())
    w = 0.35
    for i, s in enumerate(sk3):
        vals = [E[(E["skew"] == s) & (E["label"] == arm)]["wall_seconds"].mean()
                for arm in arms]
        xs = [j + (i - 0.5) * w for j in range(len(arms))]
        axD.bar(xs, vals, w, label=f"skew {int(s)}",
                color=theme["series"][i % len(theme["series"])], alpha=.85)
        base = vals[0]
        for x, v in zip(xs, vals):
            axD.annotate(f"{base/v:.2f}x", (x, v), textcoords="offset points",
                         xytext=(0, 3), ha="center", fontsize=7.5)
    axD.set_xticks(range(len(arms)))
    axD.set_xticklabels([labels[x] for x in arms], fontsize=9)
    axD.set_ylim(0, max(E["wall_seconds"]) * 1.3)
    axD.set_title("D. what actually pays off\n(x = speedup vs no fix; salting loses)")
    axD.set_ylabel("wall (s)"); axD.legend(fontsize=8); axD.grid(alpha=.3, axis="y")

    # --- E. 파티션 수 ----------------------------------------------------
    Fp = b[b["label"].str.match(r"^p\d+$", na=False)]
    g = Fp.groupby("partitions").mean(numeric_only=True).reset_index().sort_values("partitions")
    ax2 = axE.twinx()
    axE.bar([str(int(p)) for p in g["partitions"]], g["sr_bytes_max"] / MB,
            color="#8a8a86", alpha=.7, label="hot partition")
    ax2.plot([str(int(p)) for p in g["partitions"]], g["wall_seconds"],
             "o-", color="#eb6834", label="wall")
    axE.set_title("E. more partitions does not help\n"
                  "(one hot key still lands in one partition)")
    axE.set_xlabel("spark.sql.shuffle.partitions")
    axE.set_ylabel("hot partition (MiB)")
    ax2.set_ylabel("wall (s)", color="#eb6834", labelpad=2)
    ax2.tick_params(axis="y", colors="#eb6834")
    ax2.set_ylim(0, max(g["wall_seconds"]) * 1.4)
    axE.grid(alpha=.3, axis="y")

    # --- F. NULL key -----------------------------------------------------
    G = b[b["label"].isin(["inner", "outer"])]
    gg = G.groupby("label").mean(numeric_only=True)
    names2 = ["inner join", "left outer join"]
    hot = [gg.loc["inner", "sr_bytes_max"] / MB, gg.loc["outer", "sr_bytes_max"] / MB]
    wall = [gg.loc["inner", "wall_seconds"], gg.loc["outer", "wall_seconds"]]
    axF.bar(names2, hot, 0.5, color=["#1baf7a", "#eb6834"], alpha=.85)
    for i, (h, wl) in enumerate(zip(hot, wall)):
        axF.annotate(f"{h:,.0f} MiB\n{wl:.1f}s", (i, h), textcoords="offset points",
                     xytext=(0, 4), ha="center", fontsize=9, fontweight="bold")
    axF.axhline(POOL_MIB, ls="--", lw=1, color=theme["ink2"])
    axF.text(1.45, POOL_MIB, "pool 720 MiB", va="bottom", ha="right",
             fontsize=8, color=theme["ink2"])
    axF.set_ylim(0, max(hot) * 1.3)
    axF.set_title("F. NULL keys: inner is free, outer is not\n"
                  "(Spark pushes IsNotNull only when it may drop rows)")
    axF.set_ylabel("hot partition (MiB)")
    axF.grid(alpha=.3, axis="y")

    n = len(a) + len(b)
    nf = int(a["error"].notna().sum() + b["error"].notna().sum())
    fig.suptitle(f"S11 / S11b — how skew shows up and what actually fixes it   "
                 f"({n} runs, {nf} failed by design)", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, .94), h_pad=3.2, w_pad=2.0)

    outdir = os.path.join(root, "s11", "figures")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, f"practical{'_dark' if args.dark else ''}.png")
    fig.savefig(out, dpi=150)
    print(f"[plot] {out}")


if __name__ == "__main__":
    main()
