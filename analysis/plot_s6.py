#!/usr/bin/env python
"""
S6 — spill 비용은 디스크인가 CPU인가?  (RQ3)

S5 에서 vm.dirty_ratio 를 10배 조여도 job 은 꿈쩍하지 않았다. 배경 writeback 이
항상 따라잡았기 때문이다. 그래서 여기서는 **디바이스 자체를 느리게** 만든다
(cgroup v2 io.max, 쓰기만 50 MB/s).

패널
  A. max task 시간 — codec 별, io cap 유무
  B. PSI io.full — 태스크가 I/O 로 실제 멈춘 시간
  C. shuffle write 바이트 — codec 이 실제로 쓰는 양 (A/B 를 해석하는 근거)

C 가 왜 필요한가: "디스크가 느릴 때 zstd 가 이긴다"를 주장하려면 zstd 가
실제로 더 적게 쓴다는 것을 보여야 한다. 안 그러면 그냥 우연이다.
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


def codec_label(r):
    """shuffle.compress=false 는 codec 인자와 무관하게 '압축 없음'이다."""
    return "none" if not r["shuffle_compress"] else r["codec"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s6")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = pd.read_csv(os.path.join(root, args.tag, "summary.csv"))
    if "error" in df:
        df = df[df["error"].isna()]
    df = df.copy()
    df["codec_l"] = df.apply(codec_label, axis=1)
    df["psi_io_s"] = df["delta_psi_io_full_us"] / 1e6
    df["sw_mib"] = df["spill_disk_total"] / MB

    theme = DARK if args.dark else LIGHT
    style(theme)
    codecs = ["lz4", "zstd", "none"]
    codecs = [c for c in codecs if c in set(df["codec_l"])]
    colors = dict(zip(codecs, theme["series"]))
    caps = sorted(df["io_cap_mbps"].unique())
    skews = sorted(df["skew"].unique())

    panels = [("task_ms_max", "A.  max task duration  (ms)"),
              ("psi_io_s", "B.  PSI io.full  (s)"),
              ("sw_mib", "C.  spill to disk  (MiB)")]

    fig, axes = plt.subplots(1, 3, figsize=(14.5, 4.8))
    x = np.arange(len(skews) * len(caps))
    labels = [f"skew {s:g}\n{'uncapped' if c == 0 else f'{c} MB/s'}"
              for c in caps for s in skews]
    w = 0.8 / max(1, len(codecs))

    for ax, (col, title) in zip(axes, panels):
        for i, cdc in enumerate(codecs):
            vals, errs = [], []
            for c in caps:
                for s in skews:
                    g = df[(df["codec_l"] == cdc) & (df["io_cap_mbps"] == c)
                           & (df["skew"] == s)][col]
                    vals.append(g.median() if len(g) else np.nan)
                    errs.append((g.quantile(.75) - g.quantile(.25)) if len(g) > 1 else 0)
            ax.bar(x + (i - (len(codecs) - 1) / 2) * w, vals, width=w * 0.92,
                   color=colors[cdc], label=cdc, zorder=3)
            ax.errorbar(x + (i - (len(codecs) - 1) / 2) * w, vals, yerr=errs,
                        fmt="none", ecolor=theme["ink2"], elinewidth=1.1,
                        capsize=3, zorder=4)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8.5)
        ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
        ax.grid(axis="x", visible=False)
        ax.set_ylim(bottom=0)
    axes[0].legend(frameon=False, loc="upper left", labelcolor=theme["ink2"],
                   title="shuffle codec", title_fontproperties={"size": 9})

    fig.suptitle("Throttle the disk and the best codec flips",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.subplots_adjust(left=0.055, right=0.99, top=0.86, bottom=0.17, wspace=0.24)
    out = os.path.join(root, args.tag, "figures",
                       f"disk_vs_cpu{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")

    pd.set_option("display.width", 220)
    print(f"\n=== 조건별 반복 수 (n) ===")
    print(df.pivot_table(index=["io_cap_mbps", "skew"], columns="codec_l",
                         values="task_ms_max", aggfunc="count").to_string())

    print("\n=== max task duration (ms, median) ===")
    piv = df.pivot_table(index=["io_cap_mbps", "skew"], columns="codec_l",
                         values="task_ms_max", aggfunc="median").round(0)
    print(piv.to_string())

    print("\n=== PSI io.full (s, median) — 태스크가 I/O 로 멈춘 시간 ===")
    print(df.pivot_table(index=["io_cap_mbps", "skew"], columns="codec_l",
                         values="psi_io_s", aggfunc="median").round(1).to_string())

    print("\n=== spill to disk (MiB, median) — codec 이 실제로 쓰는 양 ===")
    print(df.pivot_table(index=["io_cap_mbps", "skew"], columns="codec_l",
                         values="sw_mib", aggfunc="median").round(0).to_string())

    if len(caps) > 1 and 0 in caps:
        cap = [c for c in caps if c != 0][0]
        print(f"\n=== 디스크를 {cap} MB/s 로 조였을 때 느려진 배수 ===")
        # 위에서 만든 pivot 을 그대로 쓴다. df 를 다시 필터링하면 dtype/정렬 때문에
        # 빈 선택이 되어 NaN 이 나오기 쉽다.
        for s in skews:
            row = []
            for cdc in codecs:
                try:
                    a = piv.loc[(0, s), cdc]
                    b = piv.loc[(cap, s), cdc]
                    row.append(f"{cdc}={b / a:.2f}x" if a and not np.isnan(b)
                               else f"{cdc}=n/a")
                except KeyError:
                    row.append(f"{cdc}=n/a")
            print(f"  skew {s:>4.0f}: " + "   ".join(row))
        print("\n  1.0 에 가까우면 디스크는 병목이 아니다. 크면 디스크가 병목이다.")

        print(f"\n=== 왜 그런가: spill 을 {cap} MB/s 로 쓰는 데 걸리는 시간 vs 연산 시간 ===")
        sw = df.pivot_table(index=["io_cap_mbps", "skew"], columns="codec_l",
                            values="sw_mib", aggfunc="median")
        for s in skews:
            print(f"  skew {s:g}")
            for cdc in codecs:
                try:
                    mib = sw.loc[(cap, s), cdc]
                    compute_s = piv.loc[(0, s), cdc] / 1000.0   # 제한 없을 때 = 연산 시간
                    io_s = mib / cap
                    verdict = "숨겨짐" if io_s < compute_s else "노출됨"
                    print(f"    {cdc:>5}: spill {mib:>7.0f} MiB -> I/O {io_s:>6.1f}s "
                          f"vs 연산 {compute_s:>6.1f}s   {verdict}")
                except KeyError:
                    pass
        print("\n  I/O 시간이 연산 시간보다 짧으면 그 아래로 숨어 비용이 안 보인다.")


if __name__ == "__main__":
    main()
