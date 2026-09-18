#!/usr/bin/env python
"""
S6 v3 — 1/대역폭 법칙 검증.

이전 실패에서 배운 것을 전부 반영한 분석:
  - cap 이 실제로 걸린 run 만 쓴다 (io_cap_applied, cgroup io.stat 기준)
  - median 만 보지 않고 min/max 를 같이 본다 (양봉을 median 이 뭉갰던 적 있음)
  - 분모는 Spark 자체 회계가 아니라 **cgroup 이 실제로 쓴 바이트**를 쓴다

검증할 예측
  cap 이 병목이면  wall >= cgroup_written / cap
  그리고 cap 을 반으로 줄이면 시간은 두 배가 되어야 한다.
"""
import argparse
import glob
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from plot_cliff import LIGHT, DARK, style

MB = 2 ** 20


def load(root, tag):
    rows = []
    for d in sorted(glob.glob(os.path.join(root, tag, "*", ""))):
        d = d.rstrip(os.sep)
        p = os.path.join(d, "summary.json")
        if not os.path.exists(p):
            p = os.path.join(d, "meta.json")
        if not os.path.exists(p):
            continue
        j = json.load(open(p))
        if j.get("error"):
            continue
        rows.append({
            "codec": "none" if not j.get("shuffle_compress") else j.get("codec"),
            "cap": j.get("io_cap_mbps", 0),
            "rep": j.get("rep"),
            "task_s": j.get("task_ms_max", 0) / 1000,
            "wall_s": j.get("wall_seconds", 0),
            "cg_written_MiB": (j.get("cgroup_written_bytes") or 0) / MB,
            "cg_mbps": j.get("cgroup_write_mbps"),
            "applied": j.get("io_cap_applied"),
            "spark_written_MiB": j.get("total_written_bytes", 0) / MB,
            "spill_MiB": j.get("spill_disk_total", 0) / MB,
            "psi_io_s": j.get("delta_psi_io_full_us", 0) / 1e6,
        })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s6v3")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = load(root, args.tag)
    pd.set_option("display.width", 230)

    # ---------- cap 적용 여부 ----------
    capped = df[df["cap"] != 0]
    n_bad = (capped["applied"] == False).sum()  # noqa: E712
    print(f"=== cap 적용 검증 (cgroup io.stat 기준) ===")
    print(f"  제한 run {len(capped)}개 중 미적용 {n_bad}개")
    if n_bad:
        print(df[df["applied"] == False][  # noqa: E712
            ["codec", "cap", "rep", "cg_mbps", "task_s", "wall_s"]].to_string(index=False))
        print("  → 아래 분석에서 제외한다.")

    ok = df[(df["cap"] == 0) | (df["applied"] == True)]  # noqa: E712

    # ---------- 산포 (양봉 재발 확인) ----------
    print("\n=== 조건별 산포 — 양봉이 남아 있는가 ===")
    g = ok.groupby(["codec", "cap"])
    st = pd.DataFrame({"n": g["task_s"].count(),
                       "task_min": g["task_s"].min().round(1),
                       "task_med": g["task_s"].median().round(1),
                       "task_max": g["task_s"].max().round(1),
                       "wall_med": g["wall_s"].median().round(1)})
    st["spread"] = (st["task_max"] / st["task_min"]).round(2)
    print(st.to_string())
    worst = st["spread"].max()
    print(f"  최악 산포 {worst:.2f}x — "
          + ("양봉 해소됨" if worst < 1.5 else "⚠️ 여전히 양봉. 원인 추가 조사 필요"))

    # ---------- 1/대역폭 법칙 ----------
    print("\n=== 1/대역폭 법칙:  wall >= cgroup_written / cap ===")
    print(f"{'codec':>6}{'cap':>6}{'n':>3}{'cg쓰기MiB':>11}{'예상초':>8}"
          f"{'실측wall':>9}{'비율':>7}{'task_s':>9}")
    for codec in ("lz4", "zstd", "none"):
        for cap in (0, 200, 100, 50):
            h = ok[(ok["codec"] == codec) & (ok["cap"] == cap)]
            if h.empty:
                continue
            w = h["cg_written_MiB"].median()
            wall = h["wall_s"].median()
            pred = w / cap if cap else np.nan
            ratio = wall / pred if cap and pred else np.nan
            print(f"{codec:>6}{cap:>6}{len(h):>3}{w:>11.0f}"
                  f"{(f'{pred:.0f}' if cap else '-'):>8}{wall:>9.0f}"
                  f"{(f'{ratio:.2f}' if cap else '-'):>7}{h['task_s'].median():>9.1f}")
    print("\n  비율이 1.0 근처면 대역폭이 정확히 병목이라는 뜻이다.")

    # ---------- 그림 ----------
    theme = DARK if args.dark else LIGHT
    style(theme)
    codecs = [c for c in ("lz4", "zstd", "none") if c in set(ok["codec"])]
    colors = dict(zip(codecs, theme["series"]))

    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8))
    caps = sorted([c for c in ok["cap"].unique() if c], reverse=True)

    ax = axes[0]
    for c in codecs:
        xs, ys, q1, q3 = [], [], [], []
        for cap in caps:
            h = ok[(ok["codec"] == c) & (ok["cap"] == cap)]["wall_s"]
            if h.empty:
                continue
            xs.append(cap); ys.append(h.median())
            q1.append(h.quantile(.25)); q3.append(h.quantile(.75))
        if not xs:
            continue
        ax.fill_between(xs, q1, q3, color=colors[c], alpha=0.14, linewidth=0)
        ax.plot(xs, ys, color=colors[c], marker="o", markersize=6,
                markeredgecolor=theme["surface"], markeredgewidth=1.4, label=c)
        h = ok[(ok["codec"] == c) & (ok["cap"] != 0)]
        ax.scatter(h["cap"], h["wall_s"], color=colors[c], s=12, alpha=0.35, linewidths=0)
    # 1/대역폭 기준선 (lz4 의 cgroup 쓰기량 기준)
    ref = ok[(ok["codec"] == "lz4") & (ok["cap"] != 0)]["cg_written_MiB"].median()
    xs = np.array(sorted(caps))
    ax.plot(xs, ref / xs, color=theme["ink2"], linestyle=(0, (4, 3)), linewidth=1.4,
            label=f"written / bandwidth  ({ref:.0f} MiB)")
    ax.set_xscale("log", base=2); ax.set_xticks(caps)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.invert_xaxis()
    ax.set_xlabel("write bandwidth cap  (MB/s)   — slower disk to the right")
    ax.set_ylabel("wall clock  (s)")
    ax.set_title("A.  wall time follows written / bandwidth", loc="left",
                 fontsize=11, color=theme["ink"])
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])

    ax = axes[1]
    for c in codecs:
        xs, ys = [], []
        for cap in [0] + caps:
            h = ok[(ok["codec"] == c) & (ok["cap"] == cap)]["task_s"]
            if h.empty:
                continue
            xs.append(cap if cap else max(caps) * 4)   # 무제한을 축 왼쪽 끝에
            ys.append(h.median())
        ax.plot(xs, ys, color=colors[c], marker="o", markersize=6,
                markeredgecolor=theme["surface"], markeredgewidth=1.4, label=c)
        h = ok[ok["codec"] == c]
        ax.scatter(h["cap"].replace(0, max(caps) * 4), h["task_s"],
                   color=colors[c], s=12, alpha=0.35, linewidths=0)
    ax.set_xscale("log", base=2)
    ticks = [max(caps) * 4] + caps
    ax.set_xticks(ticks)
    ax.set_xticklabels(["uncapped"] + [str(c) for c in caps])
    ax.invert_xaxis()
    ax.set_xlabel("write bandwidth cap  (MB/s)")
    ax.set_ylabel("max task duration  (s)")
    ax.set_title("B.  the hot task only blocks below a threshold", loc="left",
                 fontsize=11, color=theme["ink"])
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])

    fig.suptitle("How slow does the disk have to be before skew costs you?",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.subplots_adjust(left=0.07, right=0.99, top=0.86, bottom=0.14, wspace=0.24)
    out = os.path.join(root, args.tag, "figures",
                       f"bandwidth_law{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"\n[plot] {out}")


if __name__ == "__main__":
    main()
