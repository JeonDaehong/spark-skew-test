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
import re
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


def redact(text):
    """
    결과 파일은 public 저장소에 그대로 올라간다. Spark 예외 메시지에는
    실패한 executor 의 **사설 호스트명**이 박혀 있다
    (`ip-<a>-<b>-<c>-<d>.<region>.compute.internal`). 분석에 아무 값어치가
    없고 (실험은 전부 단일 노드다) VPC 주소 대역만 드러내므로 지운다.
    """
    if not text:
        return text
    return re.sub(r"ip-\d+-\d+-\d+-\d+(?:\.[a-z0-9.-]*compute\.internal)?",
                  "<executor-host>", text)


def read_cgroup_io_written():
    """
    이 프로세스가 속한 cgroup 이 **자기 이름으로** 디스크에 쓴 바이트.

    왜 필요한가
    ----------
    /proc/vmstat 의 pgpgout 은 **시스템 전체** 값이다. 앞 run 이 남긴 writeback 이
    이번 run 구간에 섞여 들어오므로, "이 run 이 cap 을 넘겼나"를 판정하는 데 쓰면
    앞 run 의 쓰기까지 이번 run 탓으로 돌리게 된다. 실제로 이것 때문에
    "io.max 가 26% 의 run 에서 안 걸렸다"는 결론을 냈다가 확신할 수 없게 됐다
    (docs/16). cgroup 자기 io.stat 은 그 모호함이 없다.

    반환: 전체 디바이스 합산 wbytes. cgroup v2 가 아니거나 io 컨트롤러가 없으면 None.
    """
    try:
        with open("/proc/self/cgroup") as fh:
            # cgroup v2 는 "0::/system.slice/..." 한 줄이다
            rel = fh.read().strip().split("::")[-1]
        path = os.path.join("/sys/fs/cgroup", rel.lstrip("/"), "io.stat")
        total = 0
        with open(path) as fh:
            for line in fh:                      # "259:0 rbytes=.. wbytes=.. ..."
                for field in line.split()[1:]:
                    k, _, v = field.partition("=")
                    if k == "wbytes":
                        total += int(v)
        return total
    except Exception:
        return None


def _verify_io_cap(cap_mbps, pgpgout_delta_kb, wall_seconds, cgroup_written=None):
    """
    선언한 io.max 대역폭 상한이 이 run 에 실제로 걸렸는지 사후 검증한다.

    왜 사후 검증인가
    ----------------
    스윕 시작 시 `dd oflag=direct` 로 한 번 확인하는 것으로는 부족하다.
    direct I/O 는 발행한 cgroup 의 bio 큐를 반드시 통과하지만, Spark 의 쓰기는
    버퍼드라 나중에 writeback 경로로 나간다. cgroup writeback 귀속이 성립하지
    않으면 스로틀이 **조용히** 무력화된다.

    2026-09-16 S6 v2 에서 실제로 제한 run 54 개 중 14 개(26%)가 그랬고,
    cap 이 낮을수록 실패율이 높았다 (200MB/s 0% · 100MB/s 28% · 50MB/s 50%).
    이걸 못 보고 "같은 조건인데 결과가 양봉으로 갈린다"는 없는 결론을 냈다.

    판정 기준은 cgroup 자기 io.stat 을 우선한다 (모호하지 않다).
    그게 없을 때만 시스템 전체 pgpgout 으로 대신한다 — 그 값은 앞 run 의
    잔여 writeback 이 섞이므로 참고용이다.
    """
    if not cap_mbps or not wall_seconds:
        return {"io_cap_applied": None, "avg_write_mbps": None,
                "cgroup_write_mbps": None}

    sys_avg = (pgpgout_delta_kb / 1024) / wall_seconds       # MiB/s, 시스템 전체
    cg_avg = (cgroup_written / 2 ** 20 / wall_seconds) if cgroup_written else None

    basis = cg_avg if cg_avg is not None else sys_avg
    applied = basis <= cap_mbps * 1.5
    if not applied:
        src = "cgroup io.stat" if cg_avg is not None else "system pgpgout(참고용)"
        print(f"[run] WARN: io.max 미적용 의심 — {src} 기준 평균 쓰기 "
              f"{basis:.1f} MB/s > cap {cap_mbps} MB/s.", file=sys.stderr)
    return {"io_cap_applied": applied,
            "avg_write_mbps": round(sys_avg, 1),
            "cgroup_write_mbps": round(cg_avg, 1) if cg_avg is not None else None}


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
        #
        # 주의: substring 으로 8 바이트만 모은다. 256B 행이 8B 가 되므로 shuffle 이
        # 32 배 줄어든다. S8 에서 이걸 놓쳐서 "집계는 계단이 없다"로 읽을 뻔했다 —
        # 실제로는 hot 파티션이 1054 MiB 가 아니라 52 MiB 였다. 풀 근처도 안 갔다.
        # 같은 규모로 비교하려면 agg_wide 를 쓸 것.
        out = df.groupBy("key").agg(
            F.count(F.lit(1)).alias("n"),
            F.collect_list(F.substring("payload", 1, 8)).alias("sample"),
        )
    elif args.workload == "agg_wide":
        # agg 와 같은데 payload 를 자르지 않는다. hot 파티션 크기가 sort/join 과
        # 같아져서 "같은 입력, 다른 연산자" 비교가 성립한다.
        #
        # 여기서 보려는 것
        # ----------------
        # ObjectHashAggregate 의 sort-based 폴백 임계값
        # (spark.sql.objectHashAggregate.sortBased.fallbackThreshold, 기본 128) 은
        # **맵에 들어온 키 개수**를 센다. 바이트가 아니다.
        # hot 파티션에는 키가 사실상 하나뿐이라 임계값에 영원히 안 걸린다.
        # => 폴백이 안 일어나고, 실행 메모리 풀 밖의 평범한 JVM 맵에 다 쌓인다.
        # => sort/join 을 구해주는 spill 이라는 안전밸브가 여기엔 없다.
        #
        # 예측: spill 0 을 유지하다가 어느 skew 에서 spill 없이 바로 OOM.
        # 대조: --extra-conf 로 fallbackThreshold=0 을 주면 즉시 폴백 ->
        #       UnsafeExternalSorter -> spill 하며 살아남아야 한다.
        out = df.groupBy("key").agg(
            F.count(F.lit(1)).alias("n"),
            F.collect_list("payload").alias("rows"),
        )
    elif args.workload == "window":
        # 키별 윈도우 — 현장에서 skew 로 제일 자주 터지는 "키별 중복 제거 / top-N" 패턴.
        #
        # 왜 이걸 집계 대신 쓰는가
        # -----------------------
        # groupBy 계열로는 계단을 잴 수 없다는 것이 S8b 스모크에서 드러났다:
        #   - 축약 가능한 집계(count, sum)는 map-side combine 이 hot 파티션을
        #     reduce 에 도달하기 전에 없애버린다. 계단이 생길 수가 없다.
        #   - 축약 불가능한 집계(collect_list of raw rows)는 hot key 의 **출력 한 행**이
        #     1.15 GB 가 되어 힙보다 커진다. "풀을 넘었다"와 "출력이 너무 크다"를
        #     분리할 수 없다.
        #
        # window 는 그 둘을 피한다: hot 파티션 전체가 reduce 태스크에 그대로 오고
        # (축약 없음), 출력은 입력 행마다 한 행이라 **경계가 있다**.
        # WindowExec 는 UnsafeExternalSorter 를 쓰므로 spill 이라는 안전밸브가 있다.
        # => sort/join 과 같은 계단이 나와야 한다.
        from pyspark.sql.window import Window
        w = Window.partitionBy("key").orderBy("payload")
        out = df.withColumn("rn", F.row_number().over(w))
    elif args.workload == "count":
        # 대조군: partial aggregation 이 skew 를 흡수해버리는 것을 보여주기 위한 워크로드
        out = df.groupBy("key").agg(F.count(F.lit(1)).alias("n"))
    elif args.workload in ("join_bc", "join_salt",
                           "join_isolate", "join_left"):
        # --- 실무 편 워크로드 (S11) -------------------------------------
        # dim 을 키 개수로 키운다. fact 의 키 범위(0..cold_keys)를 넘는 행은
        # 매칭되지 않는데, 그게 현실이다 — 디멘션은 보통 참조되는 것보다 크다.
        pad = max(0, args.dim_row_bytes - 16)
        dim = (spark.range(0, args.dim_keys)
               .selectExpr("cast(id as int) as key",
                           f"rpad(concat('d', cast(id as string)), {pad}, 'x') as dim_val"))

        if args.workload == "join_isolate":
            # hot key 분리 처리 — AQE 도 salting 도 못 쓸 때의 수동 해법.
            #
            #   hot key 만 떼어 broadcast join (셔플 없음)
            #   나머지는 평소대로 SortMergeJoin
            #   둘을 union
            #
            # 생성기에서 hot key 는 항상 0 이고 cold 는 1..cold_keys 다.
            # dim 에서 hot 쪽은 한 행뿐이라 broadcast 가 공짜다 — **이게 요점이다.**
            # salting 과 달리 dim 을 복제하지 않는다.
            hot = df.filter(F.col("key") == F.lit(args.hot_key))
            cold = df.filter(F.col("key") != F.lit(args.hot_key))
            dim_hot = dim.filter(F.col("key") == F.lit(args.hot_key))
            out = (cold.join(dim, "key")
                       .unionByName(hot.join(F.broadcast(dim_hot), "key")))
        elif args.workload == "join_left":
            # NULL key 확인용. inner join 은 Spark 가 조인 키에 isnotnull 을
            # 자동으로 끼워넣어 NULL 을 미리 털어낼 수 있다 (InferFiltersFromConstraints).
            # left outer 는 NULL 행을 **버릴 수 없으므로** 그 최적화가 막힌다.
            # 둘을 나란히 재면 "Spark 가 알아서 해주는가"의 답이 나온다.
            out = df.join(dim, "key", "left")
        elif args.workload == "join_bc":
            # broadcast 를 **명시적으로 강제**한다. 세션 설정의
            # autoBroadcastJoinThreshold=-1 보다 힌트가 우선한다.
            # dim 이 커지면 driver 가 전부 모아서 executor 로 뿌려야 하므로
            # 어느 크기부터 터지는지를 본다. 터지는 것이 결과다.
            out = df.join(F.broadcast(dim), "key")
        else:
            # salting: hot key 를 salt 개로 쪼개 여러 파티션에 흩는다.
            #   fact 쪽은 키마다 무작위 salt 를 붙이고,
            #   dim 쪽은 같은 행을 salt 개로 복제해 모든 salt 와 매칭되게 한다.
            # dim 이 salt 배로 커지는 것이 이 기법의 대가다.
            n = max(1, args.salt)
            fact_s = df.withColumn(
                "_k", F.concat_ws("#", F.col("key"),
                                  (F.rand(seed=42) * n).cast("int")))
            dim_s = (dim.withColumn("_s", F.explode(F.sequence(F.lit(0), F.lit(n - 1))))
                        .withColumn("_k", F.concat_ws("#", F.col("key"), F.col("_s")))
                        .drop("_s", "key"))
            out = fact_s.join(dim_s, "_k")
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
                    choices=["sort", "agg", "agg_wide", "window", "count",
                             "join", "join_bc", "join_salt",
                             "join_isolate", "join_left"])
    ap.add_argument("--dim-keys", type=int, default=10001,
                    help="join 워크로드의 디멘션 키 개수 (cold_keys + hot key 0)")
    ap.add_argument("--dim-row-bytes", type=int, default=32,
                    help="디멘션 행 폭. dim 크기 ~= dim_keys x dim_row_bytes. "
                         "broadcast 가 어디서 터지는지 보려고 키운다")
    ap.add_argument("--hot-key", type=int, default=0,
                    help="join_isolate 가 따로 뺄 키. 생성기의 hot key 는 항상 0")
    ap.add_argument("--salt", type=int, default=0,
                    help="join_salt 에서 hot key 를 쪼갤 개수. dim 이 이 배수만큼 커진다")
    ap.add_argument("--cores", default="4")
    ap.add_argument("--exec-mem", default="2g")
    ap.add_argument("--partitions", type=int, default=200)
    ap.add_argument("--aqe", type=lambda s: s.lower() == "true", default=False)
    ap.add_argument("--codec", default="lz4", choices=["lz4", "zstd", "snappy", "lzf"])
    ap.add_argument("--shuffle-compress", type=lambda s: s.lower() == "true", default=True)
    ap.add_argument("--extra-conf", action="append", default=[])
    ap.add_argument("--label", default="",
                    help="run_id 와 summary 에 붙는 자유 라벨. --extra-conf 로 건 개입은 "
                         "run_id 에 안 나타나므로, 개입군/대조군을 구분하려면 이걸 써야 한다. "
                         "안 쓰면 두 군의 run_id 가 같아져서 집계에서 섞인다.")
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
    label_tag = f"_{args.label}" if args.label else ""
    args.run_id = (f"{args.tag}_{mode_tag}{skew_tag:g}_rb{args.row_bytes}_c{args.cores}"
                   f"_m{args.exec_mem}_p{args.partitions}_aqe{int(args.aqe)}"
                   f"_{args.workload}{label_tag}_rep{args.rep}")
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
    cg_pre = read_cgroup_io_written()
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
    cg_post = read_cgroup_io_written()
    cg_written = (cg_post - cg_pre) if (cg_pre is not None and cg_post is not None) else None

    meta = {
        "run_id": args.run_id,
        "run_dir": run_dir,
        "tag": args.tag,
        "input": args.input,
        "workload": args.workload,
        "label": args.label,
        "dim_keys": args.dim_keys,
        "dim_row_bytes": args.dim_row_bytes,
        "dim_mb_est": round(args.dim_keys * args.dim_row_bytes / 2 ** 20, 1),
        "salt": args.salt,
        "extra_conf": ";".join(args.extra_conf),
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
        "error": redact(err),
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
        # --- io.max 가 이 run 에 실제로 걸렸는지 (S6 에서 크게 데였다) ---
        # dd oflag=direct 로 스윕 시작 때 한 번 검증하는 것으로는 부족하다.
        # Spark 는 버퍼드 쓰기를 쓰고, 그 writeback 이 cgroup 에 귀속되지 않으면
        # 스로틀이 조용히 무력화된다. 실제로 54 run 중 14 run(26%)이 그랬고,
        # 그 탓에 "양봉 현상"이라는 없는 결론을 냈다. docs/16 참조.
        # 그래서 run 마다 사후 검증한다: 평균 쓰기 속도가 cap 을 넘으면 미적용이다.
        "cgroup_written_bytes": cg_written,
        **_verify_io_cap(args.io_cap_mbps,
                         post.get("vm_pgpgout", 0) - pre.get("vm_pgpgout", 0),
                         wall, cg_written),
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
