#!/usr/bin/env python
"""
skew degree 와 row width 를 '독립적으로' 조절하는 데이터 생성기.

이 프로젝트의 핵심 통제 요구사항:
  - total bytes 는 항상 고정한다.
  - skew degree(S) 만 바꾸는 실험과, row width(W) 만 바꾸는 실험을 분리해야 한다.
    W 를 바꾸면 같은 bytes 에서 record 수가 반비례로 변한다 -> P2 (bytes vs records) 검증용.

skew 구성 방식
--------------
전체 N rows 중 비율 f 를 단일 hot key 에 몰아준다. 나머지는 K 개 cold key 에 균등.
shuffle partition 수 P 일 때 hot key 는 정확히 한 파티션에 떨어지므로:

    S = (hot partition rows) / (median partition rows)
      = ((1-f)N/P + fN) / ((1-f)N/P)
      = 1 + f*P/(1-f)

  =>  f = (S-1) / (P + S - 1)

payload 는 고엔트로피(sha2 기반)로 만들고 parquet 은 기본 uncompressed 로 쓴다.
압축이 걸리면 '총 bytes 고정'이라는 통제가 깨지기 때문이다.
"""
import argparse
import json
import os
import shutil
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# key/기타 컬럼이 차지하는 대략적인 고정 오버헤드(bytes). payload 길이 계산에만 사용.
FIXED_COL_BYTES = 16


def hot_fraction(skew: float, partitions: int) -> float:
    """목표 skew ratio S 를 만들기 위한 hot key 비율 f."""
    if skew <= 1.0:
        return 0.0
    return (skew - 1.0) / (partitions + skew - 1.0)


def _dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for fn in files:
            total += os.path.getsize(os.path.join(root, fn))
    return total


def calibrate_bytes_per_row(spark, *, payload_fn, key_fn, partitions, scratch,
                            compression, rows_per_file, sample_files=8):
    """
    row 당 실제 바이트를 '측정'한다. 추정하지 않는다.

    왜 필요한가
    -----------
    parquet 의 고정 오버헤드(길이 접두사, 페이지/열 메타데이터)는 row 폭에
    비례하지 않는다. 그래서 row_bytes 를 그대로 나눠 행 수를 정하면 좁은 row
    일수록 총 바이트가 더 많이 모자란다. 실측:

        rb=256 → 7.745 GiB (-3.2%)
        rb=64  → 6.773 GiB (-15.4%)   ← 12.8% 적음

    S2 의 전제는 "총 바이트 고정, record 수만 변화" 다. 위처럼 바이트가 같이
    변하면 계단 이동이 row 폭 때문인지 데이터가 적어서인지 구분할 수 없다.
    그래서 작은 샘플을 실제로 써보고 bytes/row 를 재서 행 수를 역산한다.
    """
    # 샘플은 본 데이터와 같은 모양이어야 한다.
    #  - key 를 상수로 두면 딕셔너리 인코딩이 과하게 먹어 bytes/row 를 과소평가한다.
    #  - 파일당 행 수가 다르면 parquet 푸터/row-group 오버헤드 비중이 달라진다.
    #    (샘플 2k행/파일 vs 실제 47k행/파일 이면 좁은 row 에서 4% 가량 어긋났다.)
    sample_rows = max(50_000, int(rows_per_file) * sample_files)
    tmp = os.path.join(scratch, f"_calib_{os.getpid()}")
    df = (spark.range(0, sample_rows, numPartitions=sample_files)
          .withColumn("key", key_fn(F.col("id")))
          .withColumn("payload", payload_fn(F.col("id")))
          .select("key", "payload"))
    df.write.mode("overwrite").option("compression", compression).parquet(tmp)
    bpr = _dir_bytes(tmp) / sample_rows
    shutil.rmtree(tmp, ignore_errors=True)
    return bpr


def build_record_skew(spark, *, total_bytes, hot_row_bytes, cold_row_bytes,
                      record_skew, partitions, cold_keys, out_path,
                      compression, seed, scratch):
    """
    byte 는 거의 균등한데 record 만 치우친 데이터. (S4 / 명제 P3 용)

    설계
    ----
    hot key 는 '좁은' 행을 많이, cold key 는 '넓은' 행을 갖는다.
    median 파티션의 행 수를 m, hot 파티션에 추가되는 hot key 행 수를 n_hot 이라 하면

        record skew  R = 1 + n_hot / m
        byte   skew  beta = 1 + (R-1) * hot_row_bytes / cold_row_bytes

    즉 cold_row_bytes 를 크게 잡으면 record 를 크게 치우치게 하면서도 byte 는
    거의 균등하게 유지할 수 있다. 예) hot=64B, cold=2048B, R=32 -> beta=1.97.

    AQE 의 skew 판정은 `size > skewedPartitionThresholdInBytes(256MB)` AND
    `size > skewedPartitionFactor(5) * median` 이다. 위 설계는 두 조건을
    구조적으로 둘 다 비껴간다. 그런데 S2 에 따르면 비용은 record 가 지배한다.
    => "AQE 에 안 보이는데 실제로는 비싼 파티션" 이 만들어진다.

    행 수 역산
    ----------
        B = N_cold * bpr_cold + n_hot * bpr_hot,   n_hot = (R-1) * N_cold / P
        => N_cold = B / (bpr_cold + (R-1) * bpr_hot / P)
    """
    def mk_payload(width):
        pad = max(0, width - FIXED_COL_BYTES)
        n_hash = max(1, -(-pad // 64))

        def payload(id_col):
            parts = [F.sha2(F.concat(id_col.cast("string"), F.lit(f"|{i}|{seed}")), 256)
                     for i in range(n_hash)]
            return F.substring(F.concat(*parts), 1, pad) if pad > 0 else F.lit("")
        return payload

    def cold_key(id_col):
        return F.pmod(F.hash(id_col, F.lit(seed)), F.lit(cold_keys)) + F.lit(1)

    pay_hot, pay_cold = mk_payload(hot_row_bytes), mk_payload(cold_row_bytes)

    # 두 폭 각각 실측 캘리브레이션 (파일당 행 수를 맞추기 위해 2회 수렴)
    bpr_cold = bpr_hot = None
    for _ in range(2):
        est = int(total_bytes / (bpr_cold or cold_row_bytes))
        bpr_cold = calibrate_bytes_per_row(
            spark, payload_fn=pay_cold, key_fn=cold_key, partitions=partitions,
            scratch=scratch, compression=compression,
            rows_per_file=max(1, est // partitions))
    hot_key = lambda _id: F.lit(0)          # noqa: E731 — hot key 는 단일 상수
    for _ in range(2):
        est = int(total_bytes / (bpr_hot or hot_row_bytes))
        bpr_hot = calibrate_bytes_per_row(
            spark, payload_fn=pay_hot, key_fn=hot_key,
            partitions=partitions, scratch=scratch, compression=compression,
            rows_per_file=max(1, est // partitions))

    n_cold = int(total_bytes / (bpr_cold + (record_skew - 1) * bpr_hot / partitions))
    n_hot = int((record_skew - 1) * n_cold / partitions)
    m = n_cold / partitions
    beta_expected = 1 + (record_skew - 1) * bpr_hot / bpr_cold

    print(f"[gen] record-skew mode: R={record_skew} "
          f"hot_row={hot_row_bytes}B({bpr_hot:.1f} 실측) cold_row={cold_row_bytes}B({bpr_cold:.1f} 실측)")
    print(f"[gen] n_cold={n_cold:,} n_hot={n_hot:,} median_part_rows={m:,.0f} "
          f"-> 예상 byte skew={beta_expected:.2f}")

    cold = (spark.range(0, n_cold, numPartitions=partitions)
            .withColumn("key", cold_key(F.col("id")))
            .withColumn("payload", pay_cold(F.col("id")))
            .select("key", "payload"))
    df = cold
    if n_hot > 0:
        hot = (spark.range(0, n_hot, numPartitions=partitions)
               .withColumn("key", F.lit(0))
               .withColumn("payload", pay_hot(F.col("id")))
               .select("key", "payload"))
        df = cold.unionAll(hot)

    t0 = time.time()
    df.write.mode("overwrite").option("compression", compression).parquet(out_path)

    return {
        "out_path": out_path,
        "skew_mode": "record",
        "total_bytes_target": total_bytes,
        "row_bytes": hot_row_bytes,
        "cold_row_bytes": cold_row_bytes,
        "record_skew_target": record_skew,
        "byte_skew_expected": round(beta_expected, 3),
        "n_rows": n_cold + n_hot,
        "n_hot": n_hot,
        "n_cold": n_cold,
        "partitions": partitions,
        "cold_keys": cold_keys,
        "compression": compression,
        "seed": seed,
        "bytes_per_row_measured": round(bpr_cold, 2),
        "bytes_per_row_hot_measured": round(bpr_hot, 2),
        "gen_seconds": round(time.time() - t0, 2),
    }


def build(spark, *, total_bytes, row_bytes, skew, partitions, cold_keys,
          out_path, compression, seed, scratch):
    f = hot_fraction(skew, partitions)
    pad_bytes = max(0, row_bytes - FIXED_COL_BYTES)
    # sha2(...,256) 은 64 hex chars. 필요한 길이만큼 반복 후 잘라 쓴다.
    n_hash = max(1, -(-pad_bytes // 64))

    def payload(id_col):
        parts = [F.sha2(F.concat(id_col.cast("string"), F.lit(f"|{i}|{seed}")), 256)
                 for i in range(n_hash)]
        return F.substring(F.concat(*parts), 1, pad_bytes) if pad_bytes > 0 else F.lit("")

    def cold_key(id_col):
        return F.pmod(F.hash(id_col, F.lit(seed)), F.lit(cold_keys)) + F.lit(1)

    # 행 수는 추정이 아니라 실측 bytes/row 로 역산한다 (calibrate 함수 주석 참조).
    # cold + hot 두 write 가 각각 partitions 개 파일을 내므로 실제 파일 수는 2*partitions.
    # 파일당 행 수를 샘플과 맞추기 위해 한 번 추정 -> 보정 -> 재측정한다.
    n_files = partitions * (2 if skew > 1.0 else 1)
    bpr = None
    for _ in range(2):
        est_rows = int(total_bytes / (bpr or row_bytes))
        bpr = calibrate_bytes_per_row(
            spark, payload_fn=payload, key_fn=cold_key, partitions=partitions,
            scratch=scratch, compression=compression,
            rows_per_file=max(1, est_rows // n_files))
    n_rows = int(total_bytes / bpr)
    n_hot = int(n_rows * f)
    n_cold = n_rows - n_hot

    print(f"[gen] calibrated {bpr:.1f} bytes/row (nominal {row_bytes}) "
          f"-> rows={n_rows:,} for {total_bytes/2**30:.2f}GiB")
    print(f"[gen] skew={skew} partitions={partitions} -> hot_fraction={f:.4f} "
          f"hot_rows={n_hot:,} cold_rows={n_cold:,}")

    # cold: key 를 0..cold_keys-1 에 균등 분포. hot key 와 겹치지 않게 1 부터 시작.
    cold = (
        spark.range(0, n_cold, numPartitions=partitions)
        .withColumn("key", cold_key(F.col("id")))
        .withColumn("payload", payload(F.col("id")))
        .select("key", "payload")
    )

    if n_hot > 0:
        hot = (
            spark.range(0, n_hot, numPartitions=partitions)
            .withColumn("key", F.lit(0))          # 단일 hot key
            .withColumn("payload", payload(F.col("id")))
            .select("key", "payload")
        )
        df = cold.unionAll(hot)
    else:
        df = cold

    t0 = time.time()
    (df.write
       .mode("overwrite")
       .option("compression", compression)
       .parquet(out_path))
    elapsed = time.time() - t0

    return {
        "out_path": out_path,
        "total_bytes_target": total_bytes,
        "row_bytes": row_bytes,
        "n_rows": n_rows,
        "skew_target": skew,
        "partitions": partitions,
        "cold_keys": cold_keys,
        "hot_fraction": f,
        "n_hot": n_hot,
        "n_cold": n_cold,
        "compression": compression,
        "seed": seed,
        "bytes_per_row_measured": round(bpr, 2),
        "gen_seconds": round(elapsed, 2),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--total-gb", type=float, default=2.0,
                    help="총 데이터 크기(GiB). 모든 실험에서 고정해야 한다.")
    ap.add_argument("--row-bytes", type=int, default=256,
                    help="row 폭(bytes). bytes 고정 상태로 record 수를 바꾸는 통제변수.")
    ap.add_argument("--skew", type=float, default=1.0,
                    help="목표 skew ratio (hot partition / median partition). 1.0=균등")
    ap.add_argument("--partitions", type=int, default=200,
                    help="shuffle partition 수. skew 계산식에 들어가므로 실험 내내 고정.")
    ap.add_argument("--cold-keys", type=int, default=None,
                    help="cold key 개수. 기본 partitions*50 (파티션당 ~50키로 부드럽게)")
    ap.add_argument("--compression", default="uncompressed",
                    choices=["uncompressed", "snappy", "zstd", "gzip"])
    ap.add_argument("--skew-mode", default="byte", choices=["byte", "record"],
                    help="byte=행 폭 고정, hot key 에 행을 몰아줌(바이트·레코드 동시 치우침). "
                         "record=hot key 는 좁은 행, cold key 는 넓은 행 "
                         "(바이트 거의 균등, 레코드만 치우침 — S4/P3 용)")
    ap.add_argument("--record-skew", type=float, default=1.0,
                    help="skew-mode=record 일 때의 목표 record skew R")
    ap.add_argument("--cold-row-bytes", type=int, default=2048,
                    help="skew-mode=record 일 때 cold key 의 행 폭. "
                         "클수록 byte skew 를 낮게 억제할 수 있다")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=None)
    ap.add_argument("--data-root", default=os.environ.get("SPARK_SKEW_DATA", "/tmp/spark-skew-data"))
    ap.add_argument("--scratch", default=os.environ.get("SPARK_SKEW_SCRATCH", "/tmp/spark-skew-scratch"))
    ap.add_argument("--gen-cores", default="*", help="생성에 쓸 로컬 코어 수")
    ap.add_argument("--gen-mem", default="6g")
    args = ap.parse_args()

    cold_keys = args.cold_keys or (args.partitions * 50)
    total_bytes = int(args.total_gb * (2 ** 30))
    if args.skew_mode == "record":
        default_name = (f"rec{args.record_skew:g}_rb{args.row_bytes}"
                        f"_cb{args.cold_row_bytes}_p{args.partitions}_g{args.total_gb:g}")
    else:
        default_name = (f"skew{args.skew:g}_rb{args.row_bytes}"
                        f"_p{args.partitions}_g{args.total_gb:g}")
    out = args.out or os.path.join(args.data_root, default_name)

    spark = (
        SparkSession.builder
        .appName(f"gen-skew{args.skew:g}-rb{args.row_bytes}")
        .master(f"local[{args.gen_cores}]")
        .config("spark.driver.memory", args.gen_mem)
        .config("spark.local.dir", os.path.join(args.scratch, "local"))
        .config("spark.sql.warehouse.dir", os.path.join(args.scratch, "warehouse"))
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.skew_mode == "record":
            meta = build_record_skew(
                spark,
                total_bytes=total_bytes,
                hot_row_bytes=args.row_bytes,
                cold_row_bytes=args.cold_row_bytes,
                record_skew=args.record_skew,
                partitions=args.partitions,
                cold_keys=cold_keys,
                out_path=out,
                compression=args.compression,
                seed=args.seed,
                scratch=args.scratch,
            )
        else:
            meta = build(
                spark,
                total_bytes=total_bytes,
                row_bytes=args.row_bytes,
                skew=args.skew,
                partitions=args.partitions,
                cold_keys=cold_keys,
                out_path=out,
                compression=args.compression,
                seed=args.seed,
                scratch=args.scratch,
            )
    finally:
        spark.stop()

    # 실제로 디스크에 쓰인 바이트 (통제가 지켜졌는지 확인용)
    actual = 0
    for root, _, files in os.walk(out):
        for fn in files:
            actual += os.path.getsize(os.path.join(root, fn))
    meta["actual_bytes"] = actual
    meta["actual_gb"] = round(actual / 2 ** 30, 3)
    meta["byte_control_error_pct"] = round(
        100.0 * (actual - meta["total_bytes_target"]) / meta["total_bytes_target"], 2)

    with open(os.path.join(out, "_gen_meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"[gen] wrote {meta['actual_gb']}GiB "
          f"(target {args.total_gb}GiB, error {meta['byte_control_error_pct']:+.2f}%) -> {out}")
    if abs(meta["byte_control_error_pct"]) > 2:
        print("[gen] WARNING: byte 통제 오차가 10%를 넘습니다. "
              "compression 또는 row_bytes 추정이 어긋났는지 확인하세요.", file=sys.stderr)


if __name__ == "__main__":
    main()
