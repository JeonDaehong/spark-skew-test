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
import re
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


def parse_agg_fallback(path):
    """
    ObjectHashAggregate 가 sort-based 로 폴백했는지 이벤트로그에서 직접 확인한다.

    왜 필요한가
    ----------
    `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` 를 건드리는 개입을
    한다. **개입이 걸렸다는 것을 개입 자체보다 먼저 검증해야 한다** (S6 에서 세 번
    데인 교훈). 태스크 시간으로 추정하면 안 된다. Spark 가 직접 내주는 메트릭이 있다.

    왜 reduce 스테이지만 보는가
    --------------------------
    이 임계값은 **맵에 들어온 키 개수**를 센다 (바이트가 아니다). map-side 부분집계는
    입력 split 하나에서 전체 키(수만 개)를 보므로 거의 항상 폴백한다. 반면 reduce
    스테이지는 파티션당 키가 적어 폴백하지 않는다. 우리가 보려는 hot 파티션은
    reduce 쪽에 있으므로 **전체 합계를 보면 map-side 폴백에 가려 정반대로 읽힌다.**
    실측: s8 의 agg skew32 는 stage1 67/81 폴백, stage2 0/200 폴백이었다.

    반환
    ----
      agg_op                    물리 플랜의 집계 연산자 이름
      has_peak_mem_metric       이 플랜이 "peak memory" 메트릭을 내주는가
                                (ObjectHashAggregate 는 안 내준다 — 0 은 '안 썼다'가
                                 아니라 '안 보인다'라는 뜻)
      agg_fallback_reduce       reduce 스테이지에서 폴백한 태스크 수
      agg_fallback_reduce_total reduce 스테이지 태스크 수
      agg_fallback_hot          hot 태스크(shuffle read 최대)가 폴백했는가 (0/1)
      agg_fallback_all          전 스테이지 합계 (참고용)
    """
    out = {"agg_op": None, "has_peak_mem_metric": False,
           "agg_fallback_reduce": None, "agg_fallback_reduce_total": None,
           "agg_fallback_hot": None, "agg_fallback_all": None}

    acc_ids = set()
    with open(path, errors="replace") as fh:
        for line in fh:
            if '"name":"peak memory"' in line:
                out["has_peak_mem_metric"] = True
            if out["agg_op"] is None:
                if "ObjectHashAggregate" in line:
                    out["agg_op"] = "ObjectHashAggregate"
                elif "HashAggregate" in line:
                    out["agg_op"] = "HashAggregate"
            if "sort fallback tasks" in line:
                acc_ids.update(int(m) for m in re.findall(
                    r'"name":"number of sort fallback tasks","accumulatorId":(\d+)', line))
    if not acc_ids:
        return out

    # 태스크별로 (스테이지, 폴백여부, shuffle read) 를 모은다.
    per_stage = {}          # sid -> [fallback_tasks, total_tasks, read_bytes]
    hot = None              # (read_bytes, sid, fallback)
    total = 0
    with open(path, errors="replace") as fh:
        for line in fh:
            if '"Event":"SparkListenerTaskEnd"' not in line:
                continue
            try:
                ev = json.loads(line)
            except Exception:
                continue
            sid = ev.get("Stage ID")
            fb = 0
            for a in (ev.get("Task Info") or {}).get("Accumulables", []) or []:
                if a.get("ID") in acc_ids:
                    try:
                        fb += int(a.get("Update") or 0)
                    except (TypeError, ValueError):
                        pass
            total += fb
            srm = (ev.get("Task Metrics") or {}).get("Shuffle Read Metrics") or {}
            read = srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
            st = per_stage.setdefault(sid, [0, 0, 0])
            st[0] += 1 if fb else 0
            st[1] += 1
            st[2] += read
            if read and (hot is None or read > hot[0]):
                hot = (read, sid, 1 if fb else 0)

    out["agg_fallback_all"] = total
    if hot is not None:
        # reduce 스테이지 = shuffle 을 읽은 스테이지 중 가장 많이 읽은 것
        rid = max((sid for sid, v in per_stage.items() if v[2] > 0),
                  key=lambda sid: per_stage[sid][2])
        out["agg_fallback_reduce"] = per_stage[rid][0]
        out["agg_fallback_reduce_total"] = per_stage[rid][1]
        out["agg_fallback_hot"] = hot[2]
    return out


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
        # --- job 전체가 디스크에 쓰는 양 (S6: 병목을 정하는 건 spill 이 아니라 이것) ---
        # 모든 스테이지의 shuffle write 를 합한다. map 스테이지가 데이터셋 전체를 쓰므로
        # reduce 의 spill 보다 한 자릿수 크다. docs/13 정정 · docs/14 참조.
        "sw_bytes_all_stages": sum(t["sw_bytes"] for t in tasks),
        "spill_disk_all_stages": sum(t["spill_disk"] for t in tasks),
        "total_written_bytes": sum(t["sw_bytes"] + t["spill_disk"] for t in tasks),
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
    meta.update(parse_agg_fallback(elog))
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
