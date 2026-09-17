#!/usr/bin/env python
"""
S6 v2 양봉(bimodal) 분석 — 같은 조건인데 무엇이 갈랐나?

문제
----
skew 64 / cap 50 MB/s / lz4 를 3회 돌리면 23.3s, 23.5s, 159.2s 가 나온다.
설정·데이터·코드가 전부 같은데 한 run 만 7배 느리다.
v1(n=2)은 느린 쪽이, v2(n=3)는 빠른 쪽이 중앙값이 되어 결론이 뒤집혔다.

접근
----
재실행이 필요 없다. 두 mode 의 run 이 이미 results/s6v2/ 에 있다.
각 run 의 samples.csv(0.25s 간격 커널 타임시리즈)를 나란히 놓고
"느린 run 에만 있는 것"을 찾는다.

보는 것
  dirty / writeback   페이지 캐시가 차 있었나
  procs_blocked       D-state(I/O 대기) 프로세스 수
  PSI io.full         전체가 I/O 로 멈춘 비율
  pgpgout 속도        실제로 디스크에 나간 속도
  run 시작 시각       앞 run 의 잔여 backlog 를 물려받았나  <- 유력 가설
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


def load_runs(root, tag):
    """summary.json 을 모아 조건·결과를 한 표로."""
    rows = []
    for d in sorted(glob.glob(os.path.join(root, tag, "*", ""))):
        d = d.rstrip(os.sep)
        p = os.path.join(d, "summary.json")
        if not os.path.exists(p):
            continue
        j = json.load(open(p))
        if j.get("error"):
            continue
        j["_dir"] = d
        j["codec_l"] = "none" if not j.get("shuffle_compress") else j.get("codec")
        j["task_s"] = j.get("task_ms_max", 0) / 1000
        rows.append(j)
    return pd.DataFrame(rows)


def load_samples(run_dir):
    s = pd.read_csv(os.path.join(run_dir, "samples.csv")).sort_values("t_rel")
    s = s.reset_index(drop=True)
    dt = s["t_rel"].diff()
    if "psi_io_full_total" in s:
        s["io_full_frac"] = (s["psi_io_full_total"].diff() / 1e6 / dt).clip(0, 1)
    if "vm_nr_dirty" in s:
        s["dirty_mib"] = s["vm_nr_dirty"] * PAGE_MIB
    if "vm_pgpgout" in s:
        s["out_mbps"] = (s["vm_pgpgout"].diff() / 1024) / dt
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s6v2")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = load_runs(root, args.tag)

    # ---------- 1. 양봉 조건 찾기 ----------
    g = df.groupby(["skew", "codec_l", "io_cap_mbps"])["task_s"]
    stat = pd.DataFrame({"n": g.count(), "min": g.min().round(1),
                         "median": g.median().round(1), "max": g.max().round(1)})
    stat["spread"] = (stat["max"] / stat["min"]).round(2)
    bimodal = stat[stat["spread"] >= 2].sort_values("spread", ascending=False)

    pd.set_option("display.width", 200)
    print("=== 양봉 의심 조건 (max/min >= 2배) ===")
    if bimodal.empty:
        print("  없음")
        return
    print(bimodal.to_string())

    # ---------- 2. 제일 심한 조건에서 느린/빠른 run 한 쌍 ----------
    (skew, codec, cap) = bimodal.index[0]
    sub = df[(df["skew"] == skew) & (df["codec_l"] == codec)
             & (df["io_cap_mbps"] == cap)].sort_values("task_s")
    fast, slow = sub.iloc[0], sub.iloc[-1]
    print(f"\n=== 비교 대상: skew {skew:g} / {codec} / cap {cap} MB/s ===")
    print(f"  빠른 run: {fast['task_s']:.1f}s   {os.path.basename(fast['_dir'])}")
    print(f"  느린 run: {slow['task_s']:.1f}s   {os.path.basename(slow['_dir'])}")

    keys = [("wall_seconds", "wall(s)", 1), ("spill_disk_total", "spill(MiB)", 1 / 2**20),
            ("total_written_bytes", "총쓰기(MiB)", 1 / 2**20),
            ("delta_psi_io_full_us", "PSI io.full(s)", 1e-6),
            ("delta_pgpgout_kb", "pgpgout(MiB)", 1 / 1024),
            ("peak_procs_blocked", "peak D-state", 1),
            ("peak_nr_dirty_pages", "peak dirty(MiB)", PAGE_MIB),
            ("gc_ms_total", "GC(ms)", 1), ("rep", "rep", 1)]
    print(f"\n  {'':>16}{'빠른':>12}{'느린':>12}{'배수':>9}")
    for k, label, sc in keys:
        a, b = fast.get(k, 0) * sc, slow.get(k, 0) * sc
        r = f"{b/a:.2f}x" if a else "-"
        print(f"  {label:<16}{a:>12.1f}{b:>12.1f}{r:>9}")

    # ---------- 3. 타임시리즈 겹쳐 그리기 ----------
    theme = DARK if args.dark else LIGHT
    style(theme)
    colors = {"fast": theme["series"][2], "slow": theme["series"][1]}
    rows = [("dirty_mib", "A.  dirty pages  (MiB)"),
            ("out_mbps", "B.  write-out rate  (MB/s)"),
            ("io_full_frac", "C.  fraction fully I/O-stalled"),
            ("procs_blocked", "D.  processes in D-state")]

    fig, axes = plt.subplots(len(rows), 1, figsize=(11, 10))
    for name, run in (("fast", fast), ("slow", slow)):
        s = load_samples(run["_dir"])
        for ax, (col, _t) in zip(axes, rows):
            if col in s:
                ax.plot(s["t_rel"], s[col], color=colors[name], linewidth=1.5,
                        label=f"{name}  ({run['task_s']:.0f}s task)", alpha=0.9)
    for ax, (_c, title) in zip(axes, rows):
        ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
        ax.set_xlabel("time since run start  (s)")
        ax.set_ylim(bottom=0)
        ax.margins(x=0.01)
    axes[0].legend(frameon=False, loc="upper right", labelcolor=theme["ink2"])

    fig.suptitle(f"Same settings, 7x different: what separated them?"
                 f"   (skew {skew:g}, {codec}, {cap} MB/s)",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.965))
    out = os.path.join(root, args.tag, "figures",
                       f"bimodal{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"\n[plot] {out}")

    # ---------- 4. 유력 가설: 실행 순서 ----------
    # 앞 run 이 남긴 writeback backlog 를 물려받으면 느려질 수 있다.
    # run_id 에 rep 이 들어 있고 sweep 은 rep -> cap -> codec -> skew 순으로 돈다.
    print("\n=== 느린 run 이 특정 rep/순서에 몰리는가 ===")
    slow_mask = df["task_s"] > df.groupby(["skew", "codec_l", "io_cap_mbps"])["task_s"] \
        .transform("min") * 2
    if slow_mask.any():
        print(df[slow_mask][["skew", "codec_l", "io_cap_mbps", "rep", "task_s",
                             "wall_seconds"]].sort_values("task_s").to_string(index=False))
    else:
        print("  느린 run 없음")


if __name__ == "__main__":
    main()
