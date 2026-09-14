#!/usr/bin/env bash
# ============================================================================
# S6 — spill 비용은 디스크 때문인가 CPU(압축·직렬화) 때문인가?  (RQ3)
#
# 배경
# ----
# S5 에서 vm.dirty_ratio 를 10배 낮춰 커널이 훨씬 공격적으로 flush 하게 만들었는데
# task 시간은 거의 안 변했다. 즉 **writeback 압력은 비용이 아니다.**
# 그렇다면 spill 이 비싼 이유는 둘 중 하나다:
#   (a) 디바이스 대역폭   -> 디스크를 느리게 만들면 느려져야 한다
#   (b) 압축/직렬화 CPU   -> codec 을 바꾸면 달라져야 한다
#
# Ousterhout et al. (NSDI'15) 은 job 레벨에서 "병목은 대개 CPU"라고 했다.
# 여기서는 skew 를 통제한 채, 2026 년 NVMe 위에서, spill 경로만 놓고 재확인한다.
#
# 설계 (2 x 4 x 2)
# ---------------
#   skew      16 (cliff 이전, spill 적음) · 64 (cliff 이후, spill 1.8GB)
#   codec     lz4 · zstd · snappy · none(shuffle.compress=false)
#   io cap    무제한 · 100 MB/s   (cgroup v2 io.max, systemd-run scope)
#   reps      3
#
# 판정
# ----
#   io cap 을 걸어도 안 느려짐        -> 디스크는 병목이 아니다 (Ousterhout 재확인)
#   codec 바꿀 때 크게 변함           -> CPU 가 병목
#   codec=none 이 가장 빠름           -> 압축 CPU 가 지배 (바이트가 늘어도 이득)
#   codec=none 이 io cap 하에서 느려짐 -> 대역폭과 CPU 의 교환관계가 드러남
#
# 주의: io.max 가 실제로 걸렸는지 먼저 검증한다. 안 걸린 채 돌면
#       "디스크는 병목이 아니다"라는 잘못된 결론이 나온다 (drop_caches 때와 같은 함정).
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

GB=${GB:-8}
RB=${RB:-256}
P=${P:-200}
CORES=${CORES:-4}
MEM=${MEM:-1500m}
REPS=${REPS:-3}
SKEWS=${SKEWS:-"16 64"}
CODECS=${CODECS:-"lz4 zstd none"}    # snappy 는 lz4 와 특성이 비슷해 제외
IO_CAPS=${IO_CAPS:-"0 50"}           # MB/s(쓰기만). spill 평균 쓰기가 ~75MB/s 라 50 이어야 물린다
TAG=${TAG:-s6}

# --target 을 써야 한다: SPARK_SKEW_DATA 는 마운트포인트가 아니라 그 하위 디렉토리라
# 그냥 findmnt 하면 exit 1 이 나고, set -e 때문에 스크립트가 통째로 죽는다.
# 표시용 값이므로 실패해도 진행한다 (systemd 는 경로를 알아서 디바이스로 해석한다).
DEV_PATH=$(findmnt -no SOURCE --target "$SPARK_SKEW_DATA" 2>/dev/null || echo "?")
echo "=== S6 sweep (RQ3: disk vs CPU) ==="
echo "  skews: ${SKEWS}  codecs: ${CODECS}  io caps: ${IO_CAPS} MB/s  reps: ${REPS}"
echo "  device: $DEV_PATH ($SPARK_SKEW_DATA)"
echo

# ---- io.max 가 실제로 먹는지 검증 ----
verify_cap() {
  # 한 줄에 몰아 쓰면 안 된다: `local a=$1 b=$((a*N))` 은 b 의 산술 확장이
  # a 대입보다 먼저 일어나 set -u 에서 unbound variable 로 죽는다.
  local mbps=$1
  local bps=$((mbps * 1000000))
  local out
  out=$(sudo systemd-run --scope --quiet \
        -p "IOWriteBandwidthMax=$SPARK_SKEW_DATA $bps" \
        dd if=/dev/zero of="$SPARK_SKEW_SCRATCH/_iotest" bs=1M count=600 \
        oflag=direct 2>&1 | tail -1)
  rm -f "$SPARK_SKEW_SCRATCH/_iotest"
  echo "$out"
}
echo "### io.max 검증 (cap=50 MB/s 로 600MB direct write)"
RES=$(verify_cap 50)
echo "  $RES"
MEASURED=$(echo "$RES" | grep -oP '[0-9.]+(?= MB/s)' | tail -1 || echo "")
if [ -z "$MEASURED" ]; then
  echo "  !! 측정 실패 — 중단. io.max 가 안 걸린 채 돌면 결론이 뒤집힌다."; exit 1
fi
if [ "$(echo "$MEASURED > 120" | bc -l)" = "1" ]; then
  echo "  !! cap 이 안 먹었다 (${MEASURED} MB/s > 120). 중단."
  echo "     cgroup io 컨트롤러 / 파일시스템 cgroup writeback 지원을 확인할 것."
  exit 1
fi
echo "  ok: ${MEASURED} MB/s 로 제한됨"
echo

echo "### 데이터셋"
for s in $SKEWS; do
  d="$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}"
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): skew=$s"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$GB" --row-bytes "$RB" \
      --skew "$s" --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
done
echo

start=$(date +%s)
for rep in $(seq 0 $((REPS-1))); do
  for cap in $IO_CAPS; do
    for codec in $CODECS; do
      for s in $SKEWS; do
        # codec=none 은 압축 자체를 끈다 (codec 인자는 lz4 로 두되 compress=false)
        if [ "$codec" = "none" ]; then
          CODEC_ARGS="--codec lz4 --shuffle-compress false"
        else
          CODEC_ARGS="--codec $codec --shuffle-compress true"
        fi
        CMD="python runner/run_one.py \
          --input $SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB} \
          --workload sort --skew $s --row-bytes $RB \
          --cores $CORES --exec-mem $MEM --partitions $P \
          $CODEC_ARGS --io-cap-mbps $cap --tag $TAG --rep $rep"

        if [ "$cap" = "0" ]; then
          eval "$CMD" 2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
        else
          # scope 안에서 돌려 디바이스 대역폭을 제한한다.
          # 쓰기만 제한한다 — 읽기까지 막으면 8GiB 파케이 스캔(캡 50MB/s 면 160초)이
          # 전체를 지배해서 "spill 이 느린 것"이 아니라 "입력 읽기가 느린 것"을 재게 된다.
          # --uid=ubuntu: 결과 파일 소유자를 유지해야 나중에 rsync 로 회수할 수 있다.
          sudo systemd-run --scope --quiet --uid=ubuntu \
            -p "IOWriteBandwidthMax=$SPARK_SKEW_DATA $((cap * 1000000))" \
            env SPARK_SKEW_DATA="$SPARK_SKEW_DATA" \
                SPARK_SKEW_SCRATCH="$SPARK_SKEW_SCRATCH" \
                JAVA_HOME="$JAVA_HOME" PATH="$PATH" \
            $CMD 2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
        fi
      done
    done
  done
done

echo
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s6.py --tag $TAG"
