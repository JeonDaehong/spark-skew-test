#!/usr/bin/env python
"""
S5 — cliff 을 커널이 만드는가?  (명제 P5)

커널 노브 하나(vm.dirty_ratio)만 바꾼 개입 실험. Spark 설정은 전부 고정.

패널
  A. skew 별 max task 시간, dirty_ratio 별 곡선
     -> 곡선이 겹치면 커널은 무관. 벌어지면 writeback 이 비용에 기여.
  B. peak dirty pages vs 커널이 계산한 실효 임계
     -> 개입이 실제로 먹혔는지 확인 (먹히지 않았다면 A 는 의미 없다)
  C. PSI io.full — 태스크가 I/O 로 실제로 멈춘 시간

B 가 이 그림의 핵심이다. "임계를 낮췄는데 아무 일도 안 일어났다"를 주장하려면
**임계가 실제로 낮아졌고 Dirty 가 그에 눌렸다**는 증거가 먼저 있어야 한다.
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style

MB = 2 ** 20
PAGE_MIB = 4 / 1024  # 4KiB 페이지 -> MiB


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s5")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = pd.read_csv(os.path.join(root, args.tag, "summary.csv"))
    if "error" in df:
        df = df[df["error"].isna()]
    df = df.copy()
    df["thresh_mib"] = df["dirty_threshold_pages"] * PAGE_MIB
    df["peak_dirty_mib"] = df["peak_nr_dirty_pages"] * PAGE_MIB
    df["psi_io_s"] = df["delta_psi_io_full_us"] / 1e6
    df["spill_mib"] = df["spill_disk_total"] / MB

    ratios = sorted(df["vm_dirty_ratio"].unique(), reverse=True)
    theme = DARK if args.dark else LIGHT
    style(theme)
    colors = dict(zip(ratios, theme["series"]))

    panels = [
        ("task_ms_max", "A.  max task duration  (ms)"),
        ("peak_dirty_mib", "B.  peak dirty pages  (MiB)"),
        ("psi_io_s", "C.  PSI io.full  (s)"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    for ax, (col, title) in zip(axes, panels):
        for dr in ratios:
            g = df[df["vm_dirty_ratio"] == dr]
            c = colors[dr]
            med = g.groupby("skew")[col].median()
            q1 = g.groupby("skew")[col].quantile(0.25)
            q3 = g.groupby("skew")[col].quantile(0.75)
            ax.fill_between(med.index, q1, q3, color=c, alpha=0.13, linewidth=0)
            ax.plot(med.index, med.values, color=c, marker="o", markersize=5,
                    markeredgecolor=theme["surface"], markeredgewidth=1.4,
                    label=f"dirty_ratio {dr}%", zorder=3)
            ax.scatter(g["skew"], g[col], color=c, s=9, alpha=0.28,
                       linewidths=0, zorder=2)
            if col == "peak_dirty_mib":
                # 커널이 계산한 실효 임계선. Dirty 가 이 아래로 눌렸으면 개입이 먹힌 것.
                th = g["thresh_mib"].median()
                ax.axhline(th, color=c, linestyle=(0, (4, 3)), linewidth=1.2, alpha=0.8)
                ax.annotate(f"threshold {th:.0f}", (med.index.min(), th),
                            xytext=(2, 4), textcoords="offset points",
                            color=c, fontsize=8, fontweight="bold")
        ax.set_xscale("log", base=2)
        ax.set_xticks(sorted(df["skew"].unique()))
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_xlabel("skew degree")
        ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
        ax.set_ylim(bottom=0)
        ax.margins(x=0.08)
    axes[0].legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])

    fig.suptitle("Forcing the kernel to flush 10x harder barely moves the job",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.subplots_adjust(left=0.06, right=0.99, top=0.86, bottom=0.14, wspace=0.26)
    out = os.path.join(root, args.tag, "figures",
                       f"kernel_writeback{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")

    pd.set_option("display.width", 200)
    print("\n=== 개입이 먹혔는가 (실효 임계 vs peak dirty) ===")
    t = df.groupby("vm_dirty_ratio").agg(
        임계MiB=("thresh_mib", "median"),
        peakDirtyMiB=("peak_dirty_mib", "median"),
        spillMiB=("spill_mib", "median")).round(0)
    t["임계도달률"] = (t["peakDirtyMiB"] / t["임계MiB"]).round(2)
    print(t.to_string())
    print("  임계도달률이 1.0 에 가까우면 balance_dirty_pages 동기 블로킹이 발동할 수 있는 구간.")

    print("\n=== 비용에 영향이 있는가 (skew 별 max task ms) ===")
    piv = df.pivot_table(index="skew", columns="vm_dirty_ratio",
                         values="task_ms_max", aggfunc="median")
    iqr = df.pivot_table(index="skew", columns="vm_dirty_ratio", values="task_ms_max",
                         aggfunc=lambda s: s.quantile(.75) - s.quantile(.25))
    print(piv.round(0).to_string())
    base = max(ratios)
    print(f"\n=== dirty_ratio {base}% 대비 변화율 (%) — IQR 도 함께 ===")
    for dr in ratios:
        if dr == base:
            continue
        chg = ((piv[dr] / piv[base] - 1) * 100).round(1)
        rel_iqr = (iqr[dr] / piv[base] * 100).round(1)
        print(f"  dirty_ratio {dr}%:")
        for s in piv.index:
            flag = "유의" if abs(chg[s]) > 2 * rel_iqr[s] else "노이즈 수준"
            print(f"    skew {s:>4.0f}: {chg[s]:+6.1f}%   (IQR {rel_iqr[s]:.1f}%)  {flag}")


if __name__ == "__main__":
    main()
