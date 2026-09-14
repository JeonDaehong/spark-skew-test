#!/usr/bin/env python
"""
S2 정량 분석 — 비용을 records 항과 bytes 항으로 분해한다.

S2 의 그림은 "좁은 row 가 비싸다"를 보여준다. 하지만 얼마나, 그리고 무엇 때문인지는
모델을 세워야 말할 수 있다. hot task 의 소요시간을 다음으로 회귀한다.

    duration_ms = a * (hot records) + b * (hot bytes) + c

  a 가 지배적이면 -> 비용은 record 수가 정한다 (P2 참)
  b 가 지배적이면 -> 비용은 바이트가 정한다  (P2 거짓)

총 바이트가 8GiB 로 고정돼 있으므로 두 항은 row width 를 통해서만 분리된다.
그래서 이 분해는 S2 설계(바이트 고정, record 19배 변화)가 있어야만 가능하다.

추가로 peak execution memory 의 '천장 높이'를 raw 바이트로 확인한다.
row width 마다 천장이 다르면, 천장을 정하는 것이 데이터가 아니라 자료구조라는 뜻이다.
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from plot_cliff import LIGHT, DARK, style

MB = 2 ** 20


def figure(df, coef, r2, root, dark=False):
    """
    두 장짜리 히어로 그림.
      A. 측정 vs 모델 예측 (y=x) — 모델이 맞는가
      B. row width 별 비용 구성 — record 항 vs byte 항 (누적 막대, 2계열)
    """
    a, b, _c = coef
    theme = DARK if dark else LIGHT
    style(theme)
    c_rec, c_byte = theme["series"][0], theme["series"][1]

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.6))

    ax = axes[0]
    widths = sorted(df["row_bytes"].unique())
    colors = dict(zip(widths, theme["series"]))
    X = np.column_stack([df["hot_rec"], df["hot_bytes"], np.ones(len(df))])
    pred = X @ coef
    lim = [0, max(df["task_ms_max"].max(), pred.max()) * 1.06 / 1000]
    ax.plot(lim, lim, color=theme["ink2"], linestyle=(0, (4, 3)), linewidth=1.2)
    for rb in widths:
        m = df["row_bytes"] == rb
        ax.scatter(pred[m.values] / 1000, df.loc[m, "task_ms_max"] / 1000,
                   color=colors[rb], s=34, alpha=0.8, linewidths=1.1,
                   edgecolors=theme["surface"], label=f"{rb} B/row", zorder=3)
    ax.set_xlim(lim); ax.set_ylim(lim); ax.set_aspect("equal")
    ax.set_xlabel("model:  a x records + b x bytes   (s)")
    ax.set_ylabel("measured hot-task duration  (s)")
    ax.set_title(f"A.  one model fits all three row widths   (R² = {r2:.3f})",
                 loc="left", fontsize=11, color=theme["ink"])
    ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"],
              title="row width", title_fontproperties={"size": 9})

    ax = axes[1]
    xs = np.arange(len(widths))
    rec_ms = [a * df[df["row_bytes"] == rb]["hot_rec"].median() / 1000 for rb in widths]
    byt_ms = [b * df[df["row_bytes"] == rb]["hot_bytes"].median() / 1000 for rb in widths]
    ax.bar(xs, rec_ms, width=0.58, color=c_rec, label="record-driven", zorder=3)
    # 2px 간격을 두어 두 구간이 붙어 보이지 않게 한다
    ax.bar(xs, byt_ms, width=0.58, bottom=[r * 1.012 for r in rec_ms],
           color=c_byte, label="byte-driven", zorder=3)
    for x, r, bt in zip(xs, rec_ms, byt_ms):
        ax.annotate(f"{100*r/(r+bt):.0f}%", (x, r / 2), ha="center", va="center",
                    color=theme["surface"], fontsize=10, fontweight="bold", zorder=4)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{w} B/row" for w in widths])
    ax.set_ylabel("hot-task cost  (s)")
    ax.set_title("B.  same 8 GiB, same bytes — only the row count changes",
                 loc="left", fontsize=11, color=theme["ink"])
    ax.legend(frameon=False, loc="upper right", labelcolor=theme["ink2"])
    ax.grid(axis="x", visible=False)

    fig.suptitle("Skew cost is paid per record, not per byte",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    out = os.path.join(root, "figures", f"decompose{'_dark' if dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s2_ec2")
    ap.add_argument("--results", default=None)
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.join(args.results or os.path.join(repo, "results"), args.tag)
    df = pd.read_csv(os.path.join(root, "summary.csv"))
    if "error" in df:
        df = df[df["error"].isna()]

    df = df.copy()
    df["hot_bytes"] = df["sr_bytes_max"]
    df["hot_rec"] = df["sr_records_max"]
    df["hot_mb"] = df["hot_bytes"] / MB
    df["ms_per_mb"] = df["task_ms_max"] / df["hot_mb"]

    pd.set_option("display.width", 200)

    # ---------- 1. 천장 높이 ----------
    print("=== peak execution memory 의 천장 (raw bytes) ===")
    for rb, g in df.groupby("row_bytes"):
        top = g["peak_exec_mem_max"].max()
        plateau = g.groupby("skew")["peak_exec_mem_max"].median()
        held = plateau[plateau == plateau.max()]
        print(f"  rb={rb:>5}  천장={top:>13,} bytes = {top/MB:7.1f} MiB "
              f"= {top/2**20/1024:.3f} GiB   (skew {list(held.index)} 에서 유지)")
    pool_mib = (1500 - 300) * 0.6
    print(f"  실행 풀 상한 = (1500-300)*0.6 = {pool_mib:.0f} MiB = {pool_mib*MB:,.0f} bytes")

    # ---------- 2. 비용 분해 ----------
    print("\n=== 비용 분해:  duration_ms = a*records + b*bytes + c ===")
    X = np.column_stack([df["hot_rec"], df["hot_bytes"], np.ones(len(df))])
    y = df["task_ms_max"].values
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    a, b, c = coef
    pred = X @ coef
    ss_res = ((y - pred) ** 2).sum()
    ss_tot = ((y - y.mean()) ** 2).sum()
    print(f"  a = {a*1e6:>8.2f} ns / record")
    print(f"  b = {b*1e6:>8.2f} ns / byte   ({b*MB:.1f} ms per MiB)")
    print(f"  c = {c:>8.1f} ms (상수)")
    print(f"  R^2 = {1 - ss_res/ss_tot:.4f}   (n={len(df)})")

    print("\n  row width 별 비용 구성 (median 조건 기준)")
    print(f"  {'rb':>6} {'records':>12} {'bytes(MiB)':>11} {'record항 ms':>12} "
          f"{'byte항 ms':>11} {'record 비중':>11}")
    for rb, g in df.groupby("row_bytes"):
        r = g["hot_rec"].median()
        bb = g["hot_bytes"].median()
        ta, tb = a * r, b * bb
        print(f"  {rb:>6} {r:>12,.0f} {bb/MB:>11.0f} {ta:>12.0f} {tb:>11.0f} "
              f"{100*ta/(ta+tb):>10.1f}%")

    # ---------- 3. 바이트당 비용 + 산포 ----------
    print("\n=== 바이트당 비용 (ms/MB) — median (IQR) ===")
    med = df.pivot_table(index="skew", columns="row_bytes", values="ms_per_mb", aggfunc="median")
    iqr = (df.pivot_table(index="skew", columns="row_bytes", values="ms_per_mb",
                          aggfunc=lambda s: s.quantile(.75) - s.quantile(.25)))
    out = pd.DataFrame(index=med.index)
    for col in med.columns:
        out[f"rb{col}"] = [f"{m:.2f} ({i:.2f})" for m, i in zip(med[col], iqr[col])]
    print(out.to_string())

    # ---------- 4. 계단 크기 ----------
    print("\n=== 계단: 각 row width 에서 ms/MB 의 최저 -> 이후 최고 ===")
    for rb in med.columns:
        s = med[rb]
        lo_i = s.idxmin()
        after = s[s.index > lo_i]
        if len(after) == 0:
            print(f"  rb={rb:>5}: 최저 이후 구간 없음")
            continue
        hi_i = after.idxmax()
        jump = 100 * (s[hi_i] / s[lo_i] - 1)
        noise = max(iqr[rb][lo_i], iqr[rb][hi_i])
        ratio = (s[hi_i] - s[lo_i]) / noise if noise else float("inf")
        print(f"  rb={rb:>5}: skew {lo_i:g}({s[lo_i]:.2f}) -> {hi_i:g}({s[hi_i]:.2f})  "
              f"+{jump:.0f}%   IQR 대비 {ratio:.0f}배")

    # ---------- 5. spill ----------
    print("\n=== spill to disk (MiB, median) ===")
    print((df.pivot_table(index="skew", columns="row_bytes",
                          values="spill_disk_total", aggfunc="median") / MB).round(0).to_string())

    print()
    r2 = 1 - ss_res / ss_tot
    figure(df, coef, r2, root)
    figure(df, coef, r2, root, dark=True)


if __name__ == "__main__":
    main()
