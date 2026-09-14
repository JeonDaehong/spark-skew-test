#!/usr/bin/env python
"""
stage 결과를 skew 별 median 한 표로 접는다.

cliff 판정에 필요한 것만 뽑고 단위를 사람이 읽는 단위(MB, s)로 바꾼다.
반복 산포(IQR)를 같이 내보내야 "꺾였다"를 주장할 수 있으므로 IQR 표도 같이 출력한다.
"""
import argparse
import os

import pandas as pd

MB = 2 ** 20

# (원본 컬럼, 표시 이름, 스케일)
COLS = [
    ("actual_size_skew",     "skew_act",   1),
    ("task_ms_p50",          "p50_ms",     1),
    ("task_ms_p90",          "p90_ms",     1),
    ("task_ms_max",          "max_ms",     1),
    ("sr_bytes_max",         "hotMB",      1 / MB),
    ("sr_records_max",       "hotRec",     1 / 1e6),
    ("peak_exec_mem_max",    "peakMB",     1 / MB),
    ("spill_mem_total",      "spillMemMB", 1 / MB),
    ("spill_disk_total",     "spillDskMB", 1 / MB),
    ("remote_to_disk_total", "r2dMB",      1 / MB),
    ("gc_ms_total",          "gc_ms",      1),
    ("delta_psi_io_full_us", "psi_io_s",   1e-6),
    ("peak_dirty_kb",        "dirtyMB",    1 / 1024),
    ("wall_seconds",         "wall_s",     1),
]


def table(df, how):
    cols = [(c, n, s) for c, n, s in COLS if c in df.columns]
    g = df.groupby("skew")
    out = pd.DataFrame(index=sorted(df["skew"].unique()))
    for c, name, scale in cols:
        if how == "median":
            v = g[c].median()
        else:  # iqr
            v = g[c].quantile(0.75) - g[c].quantile(0.25)
        out[name] = v * scale
    out.index.name = "skew"
    return out.round(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s1")
    ap.add_argument("--results", default=None)
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.join(args.results or os.path.join(repo, "results"), args.tag)
    df = pd.read_csv(os.path.join(root, "summary.csv"))
    if "error" in df:
        n_err = df["error"].notna().sum()
        if n_err:
            print(f"[warn] {n_err} runs failed — 제외함")
        df = df[df["error"].isna()]

    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 60)

    med = table(df, "median")
    print(f"=== {args.tag}: skew 별 MEDIAN  (n={len(df)} runs, "
          f"{df.groupby('skew').size().iloc[0]} reps/level) ===")
    print(med.to_string())

    print(f"\n=== {args.tag}: skew 별 IQR (반복 산포 — 이보다 작은 변화는 주장 불가) ===")
    print(table(df, "iqr").to_string())

    # 배증당 증가율: 이 값이 1.0 을 넘으면 그 구간이 초선형(superlinear)이다.
    print("\n=== max task duration: skew 배증당 증가 배수 / log2 지수 ===")
    m = med["max_ms"]
    rows = []
    for a, b in zip(m.index[:-1], m.index[1:]):
        ratio = m[b] / m[a] if m[a] else float("nan")
        rows.append({
            "구간": f"{a:g}→{b:g}",
            "max_ms": f"{m[a]:.0f}→{m[b]:.0f}",
            "배수": round(ratio, 2),
            "log2지수": round(pd.np.log2(ratio), 2) if hasattr(pd, "np") else round(
                __import__("math").log2(ratio), 2),
            "선형대비": "초선형" if ratio > 2.05 else ("선형" if ratio > 1.9 else "아선형"),
        })
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n(skew 를 2배 했을 때 배수가 2.0 이면 정확히 선형. "
          "2.0 을 넘는 구간이 비선형 가속 구간이다.)")


if __name__ == "__main__":
    main()
