#!/usr/bin/env python
"""
run 하나 = 결과 디렉토리 하나.

산출물:
  results/<run_id>/meta.json      실행 조건 + wall time (나중에 CSV 한 줄로 접힘)
  results/<run_id>/samples.csv    커널 타임시리즈 (sampler.py)
  results/<run_id>/eventlog/*     Spark event log  (parse/ 에서 task 메트릭 추출)

워크로드에 대하여
-----------------
기본 워크로드는 'sort' 다. groupBy().count() 같은 대수적 집계는 map-side partial
aggregation 이 hot key 를 mapper 당 1행으로 접어버려서 skew 효과 자체가 사라진다.
repartition(key) + sortWithinPartitions 는 hot 파티션의 모든 레코드가 실제로
UnsafeExternalSorter 를 통과하게 만든다. 즉 이 프로젝트가 보려는
  - LongArray(16 bytes/record) 성장
  - execution memory 고갈 -> spill
  - spill write -> page cache -> writeback
경로를 정확히 자극한다.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def drop_caches():
    """
    run 사이 page cache 초기화. 안 하면 앞 run 의 캐시가 다음 run 을 오염시킨다.

    로컬 WSL 은 root 로 돌지만 EC2 는 ubuntu 사용자다. 직접 쓰기가 막히면
    passwordless sudo 로 재시도한다. 이 통제가 꺼진 채 돌면 서로 다른 머신의
    결과를 비교할 수 없게 되므로, 실패하면 호출자가 중단할 수 있도록 False 를 준다.
    """
    try:
        subprocess.run(["sync"], check=True)
    except Exception:
        pass
    try:
        with open("/proc/sys/vm/drop_caches", "w") as fh:
            fh.write("3\n")
        return True
    except Exception:
        pass
    try:
        subprocess.run(["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
                       check=True, capture_output=True)
        return True
    except Exception as e:
        print(f"[run] WARN: drop_caches 실패 ({e}). root 또는 passwordless sudo 필요.",
              file=sys.stderr)
        return False


def read_sysctl(path):
    """/proc/sys/<path> 를 정수로. 실험 조건을 결과에 박아두기 위한 것."""
    try:
        with open("/proc/sys/" + path) as fh:
            return int(fh.read().strip())
    except Exception:
        return -1


def build_session(args, eventlog_dir):
    os.makedirs(eventlog_dir, exist_ok=True)

    # local 모드에서 driver memory 는 JVM 기동 전에 정해져야 하므로 여기서 주입한다.
    submit_args = [
        f"--driver-memory {args.exec_mem}",
        "--conf spark.driver.maxResultSize=1g",
        "pyspark-shell",
    ]
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(submit_args)

    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder
        .appName(args.run_id)
        .master(f"local[{args.cores}]")
        .config("spark.local.dir", os.path.join(args.scratch, "local"))
        .config("spark.sql.warehouse.dir", os.path.join(args.scratch, "warehouse"))
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file://" + eventlog_dir)
        # 평문 단일 파일로 남겨야 parse/ 에서 추가 의존성 없이 읽을 수 있다.
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # --- 실험 변수 ---
        .config("spark.sql.adaptive.enabled", str(args.aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(args.aqe).lower())
        .config("spark.io.compression.codec", args.codec)
        .config("spark.shuffle.compress", str(args.shuffle_compress).lower())
        .config("spark.shuffle.spill.compress", str(args.shuffle_compress).lower())
        # --- 통제 (경로가 바뀌면 비교가 깨지므로 고정) ---
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.5")
    )
    if args.extra_conf:
        for kv in args.extra_conf:
            k, _, v = kv.partition("=")
            b = b.config(k, v)
    return b.getOrCreate()


def workload(spark, args):
    from pyspark.sql import functions as F

    df = spark.read.parquet(args.input)

    if args.workload == "sort":
        # hot 파티션의 모든 레코드가 sorter 를 통과한다.
        out = (df.repartition(args.partitions, F.col("key"))
                 .sortWithinPartitions("key", "payload"))
    elif args.workload == "agg":
        # collect_list 는 비대수적이라 map-side combine 으로 접히지 않는다.
        out = df.groupBy("key").agg(
            F.count(F.lit(1)).alias("n"),
            F.collect_list(F.substring("payload", 1, 8)).alias("sample"),
        )
    elif args.workload == "count":
        # 대조군: partial aggregation 이 skew 를 흡수해버리는 것을 보여주기 위한 워크로드
        out = df.groupBy("key").agg(F.count(F.lit(1)).alias("n"))
    elif args.workload == "join":
        # AQE 의 skewJoin 은 SortMergeJoin 에만 적용된다. sort 워크로드로는
        # skew 분할 자체가 일어나지 않아 P3 를 검증할 수 없다.
        # dim 은 작지만 autoBroadcastJoinThreshold=-1 로 broadcast 를 막아 SMJ 를 강제한다.
        dim = (spark.range(0, args.dim_keys)
               .selectExpr("cast(id as int) as key",
                           "concat('d', cast(id as string)) as dim_val"))
        out = df.join(dim, "key")
    else:
        raise ValueError(args.workload)

    # noop sink: 결과를 디스크에 쓰지 않으면서 전체 연산을 강제한다.
    out.write.format("noop").mode("overwrite").save()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="gen/make_skewed.py 가 만든 parquet 경로")
    ap.add_argument("--workload", default="sort",
                    choices=["sort", "agg", "count", "join"])
    ap.add_argument("--dim-keys", type=int, default=10001,
                    help="join 워크로드의 디멘션 키 개수 (cold_keys + hot key 0)")
    ap.add_argument("--cores", default="4")
    ap.add_argument("--exec-mem", default="2g")
    ap.add_argument("--partitions", type=int, default=200)
    ap.add_argument("--aqe", type=lambda s: s.lower() == "true", default=False)
    ap.add_argument("--codec", default="lz4", choices=["lz4", "zstd", "snappy", "lzf"])
    ap.add_argument("--shuffle-compress", type=lambda s: s.lower() == "true", default=True)
    ap.add_argument("--extra-conf", action="append", default=[])
    # 라벨 (분석에서 축으로 쓰임)
    ap.add_argument("--skew", type=float, required=True)
    ap.add_argument("--row-bytes", type=int, required=True)
    ap.add_argument("--rep", type=int, default=0)
    ap.add_argument("--skew-mode", default="byte", choices=["byte", "record"],
                    help="결과 라벨. byte=바이트·레코드 동시 치우침, record=레코드만 치우침")
    ap.add_argument("--record-skew", type=float, default=1.0, help="결과 라벨")
    ap.add_argument("--io-cap-mbps", type=int, default=0,
                    help="결과 라벨. cgroup io.max 로 건 디바이스 대역폭 상한(MB/s). 0=무제한")
    ap.add_argument("--tag", default="s1")
    # 경로
    ap.add_argument("--results", default=None)
    ap.add_argument("--scratch", default=os.environ.get("SPARK_SKEW_SCRATCH", "/tmp/spark-skew-scratch"))
    ap.add_argument("--sample-interval", type=float, default=0.25)
    ap.add_argument("--no-drop-caches", action="store_true")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_root = args.results or os.path.join(repo, "results")
    mode_tag = "rec" if args.skew_mode == "record" else "byt"
    skew_tag = args.record_skew if args.skew_mode == "record" else args.skew
    args.run_id = (f"{args.tag}_{mode_tag}{skew_tag:g}_rb{args.row_bytes}_c{args.cores}"
                   f"_m{args.exec_mem}_p{args.partitions}_aqe{int(args.aqe)}"
                   f"_{args.workload}_rep{args.rep}")
    run_dir = os.path.join(results_root, args.tag, args.run_id + "_" + uuid.uuid4().hex[:6])
    os.makedirs(run_dir, exist_ok=True)
    eventlog_dir = os.path.join(run_dir, "eventlog")

    # 통제가 조용히 꺼진 채로 도는 것이 가장 위험하다. 실패하면 즉시 중단한다.
    if args.no_drop_caches:
        dropped = False
    else:
        dropped = drop_caches()
        if not dropped:
            print("[run] ABORT: page cache 통제 실패. 이 상태의 측정치는 다른 실행과 "
                  "비교할 수 없다. 의도적으로 끄려면 --no-drop-caches 를 명시할 것.",
                  file=sys.stderr)
            return 2

    from sampler import Sampler, snapshot
    pre = snapshot()
    sampler = Sampler(interval=args.sample_interval)
    sampler.start()

    spark = build_session(args, eventlog_dir)
    spark.sparkContext.setLogLevel("WARN")

    err = None
    t0 = time.time()
    try:
        workload(spark, args)
    except Exception as e:                      # 실패도 결과다 (OOM 등)
        err = f"{type(e).__name__}: {e}"
        print(f"[run] FAILED: {err}", file=sys.stderr)
    wall = time.time() - t0

    try:
        spark.stop()
    except Exception:
        pass

    sampler.stop()
    n_samples = sampler.to_csv(os.path.join(run_dir, "samples.csv"))
    post = snapshot()

    meta = {
        "run_id": args.run_id,
        "run_dir": run_dir,
        "tag": args.tag,
        "input": args.input,
        "workload": args.workload,
        "skew": args.skew,
        "skew_mode": args.skew_mode,
        "io_cap_mbps": args.io_cap_mbps,
        "record_skew": args.record_skew,
        "row_bytes": args.row_bytes,
        "cores": args.cores,
        "exec_mem": args.exec_mem,
        "partitions": args.partitions,
        "aqe": args.aqe,
        "codec": args.codec,
        "shuffle_compress": args.shuffle_compress,
        "rep": args.rep,
        "wall_seconds": round(wall, 3),
        "error": err,
        "dropped_caches": dropped,
        "n_samples": n_samples,
        # 커널 누적 카운터의 run 전후 차이 (delta 가 곧 이 run 이 유발한 양)
        "delta_pgpgout_kb": post.get("vm_pgpgout", 0) - pre.get("vm_pgpgout", 0),
        "delta_psi_io_full_us": post.get("psi_io_full_total", 0) - pre.get("psi_io_full_total", 0),
        "delta_psi_mem_full_us": post.get("psi_memory_full_total", 0) - pre.get("psi_memory_full_total", 0),
        "delta_psi_cpu_some_us": post.get("psi_cpu_some_total", 0) - pre.get("psi_cpu_some_total", 0),
        "delta_ctxt": post.get("ctxt", 0) - pre.get("ctxt", 0),
        "peak_dirty_kb": max((r.get("mem_Dirty", 0) for r in sampler.rows), default=0),
        # --- 커널 writeback 스로틀링 증거 (S5/S6) ---
        # dirty_threshold 는 커널이 계산한 실제 임계(페이지). peak_dirty 가 이걸 넘으면
        # balance_dirty_pages() 가 쓰는 스레드를 D-state 로 동기 블로킹한다.
        "vm_dirty_ratio": read_sysctl("vm/dirty_ratio"),
        "vm_dirty_background_ratio": read_sysctl("vm/dirty_background_ratio"),
        "dirty_threshold_pages": max((r.get("vm_nr_dirty_threshold", 0) for r in sampler.rows), default=0),
        "dirty_bg_threshold_pages": max((r.get("vm_nr_dirty_background_threshold", 0) for r in sampler.rows), default=0),
        "peak_nr_dirty_pages": max((r.get("vm_nr_dirty", 0) for r in sampler.rows), default=0),
        "peak_writeback_pages": max((r.get("vm_nr_writeback", 0) for r in sampler.rows), default=0),
        # D-state(uninterruptible sleep) = I/O 대기로 막힌 프로세스 수
        "peak_procs_blocked": max((r.get("procs_blocked", 0) for r in sampler.rows), default=0),
        "peak_writeback_kb": max((r.get("mem_Writeback", 0) for r in sampler.rows), default=0),
    }
    with open(os.path.join(run_dir, "meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"[run] {args.run_id}  wall={wall:.1f}s  "
          f"psi_io_full={meta['delta_psi_io_full_us']/1e6:.2f}s  "
          f"peak_dirty={meta['peak_dirty_kb']/1024:.0f}MB"
          + ("  ERROR" if err else ""))
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
