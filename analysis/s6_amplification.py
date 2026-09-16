#!/usr/bin/env python
"""
S6 의 미해명 지점: 왜 6.1 배인가?

산술 예상
---------
  skew 64 / lz4 / cap 50MB/s:  spill 1792 MiB / 50 MB/s = 35.8s
  무제한 task 시간 26.9s 이므로, I/O 가 완전히 노출돼도 ~36s 면 끝나야 한다.
실측은 164s. 4.6 배가 설명되지 않는다.

이미 배제된 가설
----------------
  "throttle -> 느려짐 -> spill 더 많이 -> I/O 더 많이" 되먹임?
  => spill_disk_total 이 cap 유무와 무관하게 1792 MiB 로 동일하다. 배제.

그래서 시간축을 본다. samples.csv 는 0.25s 간격으로
  nr_dirty / nr_writeback / procs_blocked / PSI io.full 누적
을 갖고 있으므로, 같은 조건의 capped/uncapped run 을 겹쳐 보면
언제 어디서 시간이 새는지 드러난다.
"""
import argparse
import glob
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style

PAGE_MIB = 4 / 1024


def find_runs(root, tag, **want):
    """
    run 디렉토리를 조건으로 찾는다.

    summary.json 을 우선 읽는다 — meta.json 은 실행 조건만 갖고 있고
    spill_disk_total 같은 task 집계는 parse 단계에서 summary.json 에 붙는다.
    """
    hits = []
    for d in sorted(glob.glob(os.path.join(root, tag, "*", ""))):
        d = d.rstrip(os.sep)
        path = os.path.join(d, "summary.json")
        if not os.path.exists(path):
            path = os.path.join(d, "meta.json")
            if not os.path.exists(path):
                continue
        j = json.load(open(path))
        if all(j.get(k) == v for k, v in want.items()):
            hits.append((d, j))
    return hits


def load_samples(run_dir):
    df = pd.read_csv(os.path.join(run_dir, "samples.csv"))
    df = df.sort_values("t_rel").reset_index(drop=True)
    # PSI 는 누적 마이크로초. 구간 미분해서 "이 순간 얼마나 막혀 있었나"(비율)로 바꾼다.
    for res in ("io", "memory", "cpu"):
        col = f"psi_{res}_full_total"
        if col in df:
            d = df[col].diff() / 1e6
            dt = df["t_rel"].diff()
            df[f"{res}_full_frac"] = (d / dt).clip(0, 1)
    if "vm_nr_dirty" in df:
        df["dirty_mib"] = df["vm_nr_dirty"] * PAGE_MIB
    if "vm_nr_writeback" in df:
        df["writeback_mib"] = df["vm_nr_writeback"] * PAGE_MIB
    if "vm_pgpgout" in df:
        # 실제로 디스크로 나간 양의 순간 속도 (MB/s). pgpgout 은 KB 단위 누적.
        df["out_mbps"] = (df["vm_pgpgout"].diff() / 1024) / df["t_rel"].diff()
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s6")
    ap.add_argument("--results", default=None)
    ap.add_argument("--skew", type=float, default=64.0)
    ap.add_argument("--codec", default="lz4")
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")

    pairs = {}
    for cap in (0, 50):
        hits = find_runs(root, args.tag, skew=args.skew, codec=args.codec,
                         io_cap_mbps=cap, shuffle_compress=True)
        hits = [(d, j) for d, j in hits if not j.get("error")]
        if hits:
            pairs[cap] = hits[0]
    if len(pairs) < 2:
        raise SystemExit(f"두 조건을 못 찾음: {list(pairs)}")

    theme = DARK if args.dark else LIGHT
    style(theme)
    colors = {0: theme["series"][2], 50: theme["series"][1]}
    labels = {0: "uncapped", 50: "50 MB/s cap"}

    rows = [("dirty_mib", "A.  dirty pages  (MiB)"),
            ("out_mbps", "B.  actual write-out rate  (MB/s)"),
            ("io_full_frac", "C.  fraction of time fully I/O-stalled")]

    fig, axes = plt.subplots(3, 1, figsize=(11, 8.2), sharex=False)
    summary = {}
    for cap, (d, j) in sorted(pairs.items()):
        s = load_samples(d)
        summary[cap] = dict(
            wall=j["wall_seconds"], task_max=j.get("task_ms_max"),
            spill_mib=j.get("spill_disk_total", 0) / 2 ** 20,
            psi_io=j["delta_psi_io_full_us"] / 1e6,
            pgpgout_mib=j.get("delta_pgpgout_kb", 0) / 1024,
            samples=len(s),
        )
        for ax, (col, _t) in zip(axes, rows):
            if col in s:
                ax.plot(s["t_rel"], s[col], color=colors[cap], linewidth=1.6,
                        label=labels[cap], alpha=0.9)

    for ax, (col, title) in zip(axes, rows):
        ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
        ax.set_xlabel("time since run start  (s)")
        ax.set_ylim(bottom=0)
        ax.margins(x=0.01)
    axes[0].legend(frameon=False, loc="upper right", labelcolor=theme["ink2"])

    fig.suptitle(f"Where the extra time goes  (skew {args.skew:g}, {args.codec})",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    out = os.path.join(root, args.tag, "figures",
                       f"amplification{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")

    print(f"\n=== skew {args.skew:g} / {args.codec} — 두 조건 비교 ===")
    print(f"{'':>16}{'uncapped':>14}{'50 MB/s':>14}{'배수':>10}")
    for k, unit in (("wall", "s"), ("task_max", "ms"), ("spill_mib", "MiB"),
                    ("psi_io", "s"), ("pgpgout_mib", "MiB")):
        a, b = summary[0][k], summary[50][k]
        r = (b / a) if a else float("nan")
        print(f"  {k:<14}{a:>14.1f}{b:>14.1f}{r:>9.2f}x   ({unit})")

    print("\n=== 산술 예상 vs 실측 ===")
    spill = summary[50]["spill_mib"]
    print(f"  spill {spill:.0f} MiB 를 50 MB/s 로 쓰면      = {spill/50:6.1f}s")
    print(f"  무제한 task 시간                          = {summary[0]['task_max']/1000:6.1f}s")
    print(f"  => 완전 노출돼도 예상 최대                = {max(spill/50, summary[0]['task_max']/1000):6.1f}s")
    print(f"  실측 task 시간                            = {summary[50]['task_max']/1000:6.1f}s")
    print(f"  설명 안 되는 초과분                       = "
          f"{summary[50]['task_max']/1000 - max(spill/50, summary[0]['task_max']/1000):6.1f}s")

    print("\n=== 실제로 디스크로 나간 총량 (pgpgout) ===")
    print(f"  uncapped {summary[0]['pgpgout_mib']:8.0f} MiB")
    print(f"  capped   {summary[50]['pgpgout_mib']:8.0f} MiB")
    print("  spill 만이 아니라 shuffle write 등 이 run 이 유발한 모든 쓰기가 포함된다.")
    print("  이 값이 spill 보다 훨씬 크면, cap 이 spill 이 아니라 '전체 쓰기'를 조인 것이다.")


if __name__ == "__main__":
    main()
