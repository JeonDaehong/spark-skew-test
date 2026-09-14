#!/usr/bin/env python
"""
S4 — AQE 는 record-skew 를 보는가?  (명제 P3)

패널
  A. AQE 구제 산점도: x = AQE off 의 hot 파티션 MB, y = AQE on 의 MB
     대각선 아래로 떨어지면 AQE 가 쪼갠 것, 대각선 위에 남으면 방치된 것.
  B. byte-skew 군: skew 별 max task 시간, AQE off vs on
  C. record-skew 군: R 별 max task 시간, AQE off vs on

B/C 를 나눈 이유: 두 군은 x축의 의미가 다르다(byte skew vs record skew).
같은 축에 겹쳐 그리면 없는 비교를 만들어낸다.
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from plot_cliff import LIGHT, DARK, style

MB = 2 ** 20


def load(root, tag):
    df = pd.read_csv(os.path.join(root, tag, "summary.csv"))
    if "error" in df:
        df = df[df["error"].isna()]
    df = df.copy()
    df["level"] = df.apply(
        lambda r: r["record_skew"] if r["skew_mode"] == "record" else r["skew"], axis=1)
    df["hot_mb"] = df["sr_bytes_max"] / MB
    df["hot_mrec"] = df["sr_records_max"] / 1e6
    return df


def arm_panel(ax, g, theme, title, xlabel):
    """한 군의 AQE off/on 비교."""
    for aqe, color, label in ((False, theme["series"][1], "AQE off"),
                              (True, theme["series"][0], "AQE on")):
        h = g[g["aqe"] == aqe]
        if h.empty:
            continue
        med = h.groupby("level")["task_ms_max"].median()
        q1 = h.groupby("level")["task_ms_max"].quantile(0.25)
        q3 = h.groupby("level")["task_ms_max"].quantile(0.75)
        ax.fill_between(med.index, q1, q3, color=color, alpha=0.14, linewidth=0)
        ax.plot(med.index, med.values, color=color, marker="o", markersize=5,
                markeredgecolor=theme["surface"], markeredgewidth=1.4,
                label=label, zorder=3)
        ax.scatter(h["level"], h["task_ms_max"], color=color, s=9,
                   alpha=0.30, linewidths=0, zorder=2)
    ax.set_xscale("log", base=2)
    ax.set_xticks(sorted(g["level"].unique()))
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel(xlabel)
    ax.set_ylabel("max task duration  (ms)")
    ax.set_title(title, loc="left", fontsize=11, color=theme["ink"])
    ax.set_ylim(bottom=0)
    ax.margins(x=0.08)
    ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s4b")
    ap.add_argument("--results", default=None)
    ap.add_argument("--dark", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = args.results or os.path.join(repo, "results")
    df = load(root, args.tag)

    theme = DARK if args.dark else LIGHT
    style(theme)
    c_byte, c_rec = theme["series"][1], theme["series"][0]

    fig = plt.figure(figsize=(14, 4.6))
    gs = fig.add_gridspec(1, 3, width_ratios=[1.15, 1, 1], wspace=0.3)

    # ---- A. AQE 구제 산점도 ----
    ax = fig.add_subplot(gs[0, 0])
    piv = df.pivot_table(index=["skew_mode", "level"], columns="aqe",
                         values="hot_mb", aggfunc="median")
    if True in piv.columns and False in piv.columns:
        lim = [0, piv[False].max() * 1.08]
        ax.plot(lim, lim, color=theme["ink2"], linestyle=(0, (4, 3)), linewidth=1.3,
                zorder=1)
        ax.annotate("AQE did nothing", (lim[1] * 0.30, lim[1] * 0.33),
                    color=theme["ink2"], fontsize=9, rotation=40, ha="center")
        for mode, color, label in (("byte", c_byte, "byte-skew"),
                                   ("record", c_rec, "record-skew")):
            if mode not in piv.index.get_level_values(0):
                continue
            sub = piv.loc[mode]
            ax.scatter(sub[False], sub[True], color=color, s=70, alpha=0.85,
                       linewidths=1.4, edgecolors=theme["surface"],
                       label=label, zorder=3)
            for lv, row in sub.iterrows():
                ax.annotate(f"{lv:g}", (row[False], row[True]),
                            xytext=(7, -3), textcoords="offset points",
                            color=color, fontsize=8.5, fontweight="bold")
        ax.set_xlim(lim)
        ax.set_ylim(0, max(lim[1] * 0.25, piv[True].max() * 1.3))
    ax.set_xlabel("hot partition, AQE off  (MB)")
    ax.set_ylabel("hot partition, AQE on  (MB)")
    ax.set_title("A.  does AQE split it?", loc="left", fontsize=11, color=theme["ink"])
    ax.legend(frameon=False, loc="upper left", labelcolor=theme["ink2"])

    # ---- B, C. 군별 AQE off/on ----
    arm_panel(fig.add_subplot(gs[0, 1]), df[df["skew_mode"] == "byte"], theme,
              "B.  byte-skew arm", "byte skew degree")
    arm_panel(fig.add_subplot(gs[0, 2]), df[df["skew_mode"] == "record"], theme,
              "C.  record-skew arm", "record skew R   (byte skew <= 2.6)")

    fig.suptitle("AQE rescues byte-skew. It never even sees record-skew.",
                 x=0.006, ha="left", color=theme["ink"], fontsize=13.5)
    fig.subplots_adjust(left=0.06, right=0.99, top=0.86, bottom=0.14)
    out = os.path.join(root, args.tag, "figures",
                       f"aqe_blindspot{'_dark' if args.dark else ''}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] {out}")

    # ---- 판정표 ----
    pd.set_option("display.width", 200)
    print("\n=== AQE 감지 여부 (조건별, on 만) ===")
    on = df[df["aqe"]]
    t = on.groupby(["skew_mode", "level"]).agg(
        감지=("aqe_split_detected", "max"),
        smj_skew=("smj_skew", "max"),
        read_skewed=("aqe_read_skewed", "max"),
        byte_skew=("actual_size_skew", "median"),
        hotMB=("hot_mb", "median"),
        hotMrec=("hot_mrec", "median")).round(2)
    print(t.to_string())

    print("\n=== AQE off 대비 on (구제 효과) ===")
    p = df.pivot_table(index=["skew_mode", "level"], columns="aqe",
                       values=["task_ms_max", "hot_mb"], aggfunc="median")
    o = pd.DataFrame(index=p.index)
    o["hotMB_off"] = p[("hot_mb", False)].round(0)
    o["hotMB_on"] = p[("hot_mb", True)].round(0)
    o["hot_ratio"] = (p[("hot_mb", True)] / p[("hot_mb", False)]).round(2)
    o["ms_off"] = p[("task_ms_max", False)].round(0)
    o["ms_on"] = p[("task_ms_max", True)].round(0)
    o["ms_ratio"] = (p[("task_ms_max", True)] / p[("task_ms_max", False)]).round(2)
    print(o.to_string())

    print("\n=== record-skew 군: 비용이 실제로 오르는가 (AQE off 기준) ===")
    r = df[(df["skew_mode"] == "record") & (~df["aqe"])]
    if not r.empty:
        s = r.groupby("level").agg(
            hotMrec=("hot_mrec", "median"), hotMB=("hot_mb", "median"),
            ms=("task_ms_max", "median"),
            ms_iqr=("task_ms_max", lambda x: x.quantile(.75) - x.quantile(.25))).round(2)
        s["vs_R1"] = (s["ms"] / s["ms"].iloc[0]).round(2)
        print(s.to_string())
        print("\n  vs_R1 이 1.0 근처면 record-skew 에 비용이 없다는 뜻 "
              "(= 사각지대이지만 무해). 크게 오르면 사각지대에 비용이 있다는 뜻.")


if __name__ == "__main__":
    main()
