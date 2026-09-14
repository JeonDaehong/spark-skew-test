#!/usr/bin/env python
"""
Spark event log -> task 단위 메트릭 + run 단위 요약 한 줄.

results/<tag>/<run_dir>/ 각각에 대해:
  tasks.csv    task 하나 = 한 줄 (duration, spill, shuffle, GC, ...)
  summary.json meta.json + task 집계

그리고 전체를 모아 results/<tag>/summary.csv (run 하나 = 한 줄) 로 접는다.
분석/플롯은 전부 이 CSV 하나에서 출발한다.
"""
import argparse
import csv
import glob
import json
import os
import statistics
import sys


def find_eventlog(run_dir):
    """평문 단일 파일(권장) 또는 v2 롤링 디렉토리 모두 지원."""
    cands = []
    for pat in ("eventlog/*", "eventlog/*/*"):
        for p in glob.glob(os.path.join(run_dir, pat)):
            if os.path.isfile(p) and not p.endswith((".zstd", ".lz4", ".snappy")) \
               and "appstatus" not in os.path.basename(p):
                cands.append(p)
    if not cands:
        return None
    return max(cands, key=os.path.getsize)


def parse_aqe(path):
    """
    AQE 가 실제로 skew 파티션을 쪼갰는지 이벤트로그에서 직접 확인한다.

    task 수로 추정하면 안 된다 — AQE 는 coalesce 로 task 를 '줄이기도' 하므로
    task 수 변화만 보면 분할과 병합을 구분할 수 없다. 대신 실행 플랜 문자열에서
    Spark 가 직접 표시하는 두 마커를 찾는다.

      isSkewJoin=true   SortMergeJoin 노드가 skew join 으로 재작성됨
      skewed=true       AQEShuffleRead 가 skew 파티션을 분할함

    둘 중 하나라도 있으면 AQE 가 skew 를 '보았다'는 뜻이다. 하나도 없으면
    (AQE 가 켜져 있는데도) skew 가 감지되지 않은 것이다 — 이것이 P3 의 검증 지점.
    """
    found = {"smj_skew": False, "aqe_read_skewed": False,
             "aqe_read_coalesced": False, "aqe_updates": 0}
    with open(path, errors="replace") as fh:
        for line in fh:
            if "Adaptive" in line:
                found["aqe_updates"] += 1
            if "AQEShuffleRead" not in line and "skew=" not in line:
                continue
            # Spark 4.0 실측 마커 (isSkewJoin 이 아니다 — 버전마다 다르므로 실측 확인 필수):
            #   SortMergeJoin ... skew=true)          조인이 skew join 으로 재작성됨
            #   AQEShuffleRead coalesced and skewed   shuffle read 가 skew 파티션을 분할함
            #   AQEShuffleRead coalesced              병합만 함 (skew 처리 아님)
            if "skew=true" in line:
                found["smj_skew"] = True
            if "coalesced and skewed" in line or "AQEShuffleRead skewed" in line:
                found["aqe_read_skewed"] = True
            elif "AQEShuffleRead coalesced" in line:
                found["aqe_read_coalesced"] = True
    found["aqe_split_detected"] = found["smj_skew"] or found["aqe_read_skewed"]
    return found


def parse_tasks(path):
    """SparkListenerTaskEnd 이벤트에서 task 메트릭을 뽑는다."""
    tasks = []
    stage_names = {}
    with open(path, errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            ev = e.get("Event", "")

            if ev == "SparkListenerStageSubmitted":
                si = e.get("Stage Info", {})
                stage_names[si.get("Stage ID")] = si.get("Stage Name", "")

            elif ev == "SparkListenerTaskEnd":
                ti = e.get("Task Info", {}) or {}
                tm = e.get("Task Metrics", {}) or {}
                sr = tm.get("Shuffle Read Metrics", {}) or {}
                sw = tm.get("Shuffle Write Metrics", {}) or {}
                ir = tm.get("Input Metrics", {}) or {}
                tasks.append({
                    "stage_id": e.get("Stage ID"),
                    "stage_name": stage_names.get(e.get("Stage ID"), ""),
                    "task_id": ti.get("Task ID"),
                    "index": ti.get("Index"),
                    "attempt": ti.get("Attempt"),
                    "launch_time": ti.get("Launch Time"),
                    "finish_time": ti.get("Finish Time"),
                    "duration_ms": (ti.get("Finish Time", 0) or 0) - (ti.get("Launch Time", 0) or 0),
                    "failed": bool(ti.get("Failed")),
                    "executor_run_ms": tm.get("Executor Run Time", 0),
                    "executor_cpu_ns": tm.get("Executor CPU Time", 0),
                    "deser_ms": tm.get("Executor Deserialize Time", 0),
                    "gc_ms": tm.get("JVM GC Time", 0),
                    "result_size": tm.get("Result Size", 0),
                    "peak_exec_mem": tm.get("Peak Execution Memory", 0),
                    "spill_mem": tm.get("Memory Bytes Spilled", 0),
                    "spill_disk": tm.get("Disk Bytes Spilled", 0),
                    "input_bytes": ir.get("Bytes Read", 0),
                    "input_records": ir.get("Records Read", 0),
                    "sr_bytes": (sr.get("Remote Bytes Read", 0) or 0) + (sr.get("Local Bytes Read", 0) or 0),
                    "sr_records": sr.get("Total Records Read", 0),
                    "sr_fetch_wait_ms": sr.get("Fetch Wait Time", 0),
                    "sr_remote_to_disk": sr.get("Remote Bytes Read To Disk", 0),
                    "sw_bytes": sw.get("Shuffle Bytes Written", 0),
                    "sw_records": sw.get("Shuffle Records Written", 0),
                    "sw_time_ns": sw.get("Shuffle Write Time", 0),
                })
    return tasks


def summarize(tasks):
    """
    핵심은 'shuffle read 를 하는 스테이지'의 task 분포다.
    skew 의 효과는 거기에 나타난다. (map 스테이지는 대체로 균등)
    """
    if not tasks:
        return {}

    def q(vals, p):
        if not vals:
            return 0
        vals = sorted(vals)
        i = min(len(vals) - 1, int(round(p * (len(vals) - 1))))
        return vals[i]

    # shuffle read 가 있는 스테이지 중 task 수가 가장 많은 것을 reduce 스테이지로 본다
    by_stage = {}
    for t in tasks:
        by_stage.setdefault(t["stage_id"], []).append(t)
    reduce_stage, reduce_tasks = None, []
    for sid, ts in by_stage.items():
        if sum(t["sr_bytes"] for t in ts) > 0 and len(ts) > len(reduce_tasks):
            reduce_stage, reduce_tasks = sid, ts
    if not reduce_tasks:
        reduce_stage, reduce_tasks = max(by_stage.items(), key=lambda kv: len(kv[1]))

    d = [t["duration_ms"] for t in reduce_tasks]
    sr = [t["sr_bytes"] for t in reduce_tasks]
    med_d = statistics.median(d) if d else 0
    med_sr = statistics.median(sr) if sr else 0

    return {
        "n_tasks_total": len(tasks),
        "reduce_stage_id": reduce_stage,
        "n_reduce_tasks": len(reduce_tasks),
        # --- task duration 분포 (cliff 곡선의 y축 후보들) ---
        "task_ms_p50": med_d,
        "task_ms_p90": q(d, 0.90),
        "task_ms_p99": q(d, 0.99),
        "task_ms_max": max(d) if d else 0,
        "task_ms_sum": sum(d),
        # 실측 skew (목표 skew 가 실제로 구현됐는지 확인 — B1)
        "actual_size_skew": (max(sr) / med_sr) if med_sr else 0,
        "actual_time_skew": (max(d) / med_d) if med_d else 0,
        # --- 비용 분해 ---
        "spill_mem_total": sum(t["spill_mem"] for t in reduce_tasks),
        "spill_disk_total": sum(t["spill_disk"] for t in reduce_tasks),
        "spill_disk_max_task": max((t["spill_disk"] for t in reduce_tasks), default=0),
        "gc_ms_total": sum(t["gc_ms"] for t in reduce_tasks),
        "gc_ms_max_task": max((t["gc_ms"] for t in reduce_tasks), default=0),
        "peak_exec_mem_max": max((t["peak_exec_mem"] for t in reduce_tasks), default=0),
        "cpu_ms_total": sum(t["executor_cpu_ns"] for t in reduce_tasks) // 1_000_000,
        "run_ms_total": sum(t["executor_run_ms"] for t in reduce_tasks),
        "fetch_wait_ms_total": sum(t["sr_fetch_wait_ms"] for t in reduce_tasks),
        # 200MB 초과 블록이 디스크로 fetch 된 양 (RQ7 — Spark UI 가 spill 로 안 세는 I/O)
        "remote_to_disk_total": sum(t["sr_remote_to_disk"] for t in reduce_tasks),
        "sr_bytes_max": max(sr) if sr else 0,
        "sr_bytes_median": med_sr,
        "sr_records_max": max((t["sr_records"] for t in reduce_tasks), default=0),
    }


def process_run(run_dir, write_tasks=True):
    meta_path = os.path.join(run_dir, "meta.json")
    if not os.path.exists(meta_path):
        return None
    meta = json.load(open(meta_path))

    elog = find_eventlog(run_dir)
    if not elog:
        meta["parse_error"] = "eventlog not found"
        return meta

    tasks = parse_tasks(elog)
    if write_tasks and tasks:
        cols = list(tasks[0].keys())
        with open(os.path.join(run_dir, "tasks.csv"), "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerows(tasks)

    meta.update(summarize(tasks))
    meta.update(parse_aqe(elog))
    with open(os.path.join(run_dir, "summary.json"), "w") as fh:
        json.dump(meta, fh, indent=2)
    return meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=None)
    ap.add_argument("--tag", default="s1")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.join(args.results or os.path.join(repo, "results"), args.tag)
    if not os.path.isdir(root):
        print(f"no such tag dir: {root}", file=sys.stderr)
        return 1

    rows = []
    for d in sorted(os.listdir(root)):
        rd = os.path.join(root, d)
        if os.path.isdir(rd):
            r = process_run(rd)
            if r:
                rows.append(r)

    if not rows:
        print("no runs parsed", file=sys.stderr)
        return 1

    cols = sorted({k for r in rows for k in r})
    order = ["run_id", "skew", "row_bytes", "cores", "exec_mem", "partitions",
             "aqe", "codec", "rep", "wall_seconds"]
    cols = order + [c for c in cols if c not in order]
    out = os.path.join(root, "summary.csv")
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"[parse] {len(rows)} runs -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
