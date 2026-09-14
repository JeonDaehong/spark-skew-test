#!/usr/bin/env python
"""summary.csv 를 터미널에서 빠르게 훑어보기 위한 도구."""
import argparse
import os

import pandas as pd

DEFAULT_COLS = [
    "skew", "row_bytes", "cores", "exec_mem", "rep",
    "wall_seconds", "n_reduce_tasks",
    "actual_size_skew", "actual_time_skew",
    "task_ms_p50", "task_ms_p90", "task_ms_max",
    "sr_bytes_max", "spill_disk_total", "gc_ms_total",
    "peak_exec_mem_max", "remote_to_disk_total",
    "delta_psi_io_full_us", "peak_dirty_kb",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="s1")
    ap.add_argument("--results", default=None)
    ap.add_argument("--cols", default=None, help="쉼표 구분 컬럼 목록")
    ap.add_argument("--sort", default="skew")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(args.results or os.path.join(repo, "results"),
                        args.tag, "summary.csv")
    df = pd.read_csv(path)

    cols = args.cols.split(",") if args.cols else DEFAULT_COLS
    cols = [c for c in cols if c in df.columns]
    sort_cols = [c for c in args.sort.split(",") if c in df.columns]

    out = df[cols].sort_values(sort_cols) if sort_cols else df[cols]
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 50)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
