#!/usr/bin/env bash
# ============================================================================
# S2 — 계단의 위치를 정하는 것은 bytes 인가 records 인가?  (명제 P2)
#
# S1 결과: 바이트당 비용이 실행 메모리 풀 포화 지점(712MB/천장 720MB)에서
#          +31% 계단. 즉 "천장"은 bytes 단위로 보인다.
#
# 그런데 UnsafeInMemorySorter 의 LongArray 는 record 당 16 bytes 다.
# 같은 데이터 바이트라도 row 가 좁으면 record 가 많아지고, 포인터 배열이 실행
# 메모리를 더 많이 잡아먹는다.
#
#   rb=256 → records = B/256 → pointer array = 16B/256 = B/16   (약 6%)
#   rb=64  → records = B/64  → pointer array = 16B/64  = B/4    (약 25%)
#
# 예측
# ----
#   bytes 가 범인이면  → 계단 위치는 row width 와 무관 (제자리)
#   records 가 범인이면 → row 가 좁을수록 계단이 더 낮은 skew 로 당겨온다
#
# 설계
# ----
#   총 바이트 8GiB 고정. row width 만 바꿔 record 수를 4배씩 움직인다.
#   skew 1,2 는 S1 에서 노이즈 지배로 판명되어 제외.
#   rb=256 은 S1 결과를 그대로 재사용한다 (다른 조건이 전부 동일).
#
#   디스크: 배치(5 dataset = 40GB) 단위로 생성 → 실행 → 삭제.
#           전부 미리 만들면 C: 여유(146GB)를 넘긴다.
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

# --- 완료 표식 (env/ec2-sync.sh wait 가 이걸 본다) -------------------------
# pgrep 으로 생사를 추정하면 안 된다: `pgrep -f x_sweep.sh` 는 자기 명령줄에도
# 매칭돼서 끝나도 RUNNING 을 반환한다. 그 버그로 결과를 통째로 잃은 적이 있다.
# 정상 종료든 실패든 반드시 표식을 남긴다 (trap) — 안 그러면 wait 가 매달린다.
_DONE_MARK="/home/ubuntu/.$(basename "$0").done"
rm -f "$_DONE_MARK"
trap 'echo "exit=$?" > "$_DONE_MARK"' EXIT

GB=${GB:-8}
P=${P:-200}
CORES=${CORES:-4}
MEM=${MEM:-1500m}
REPS=${REPS:-5}
SKEWS=${SKEWS:-"4 8 16 32 64"}
ROW_BYTES=${ROW_BYTES:-"64 1024"}     # 256 은 S1 재사용
TAG=${TAG:-s2}
# 배치마다 데이터셋을 지울지. 로컬(C: 여유 146GB)에서는 필수였지만
# EC2 인스턴스스토어는 885GB 라 지울 필요가 없다. CLEANUP=0 이면 보존한다
# (재실행 시 생성 시간을 통째로 아낀다).
CLEANUP=${CLEANUP:-1}

echo "=== S2 sweep ==="
echo "  total=${GB}GiB partitions=${P} cores=${CORES} mem=${MEM}"
echo "  row widths: ${ROW_BYTES}   skews: ${SKEWS}   reps: ${REPS}"
echo "  (row_bytes=256 은 S1 결과 재사용)"
echo

start=$(date +%s)

for RB in $ROW_BYTES; do
  echo "###############################################"
  echo "### row_bytes = ${RB}"
  echo "###############################################"

  echo "--- generate ($(echo $SKEWS | wc -w) datasets)"
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
  df -h "$SPARK_SKEW_DATA" | tail -1 | awk '{print "  disk: "$3" used, "$4" free"}'

  echo "--- run ($(echo $SKEWS | wc -w) skews x ${REPS} reps)"
  # rep 을 바깥 루프에: 시간 순서 효과가 skew 와 교락되지 않게 한다 (S1 과 동일)
  for rep in $(seq 0 $((REPS-1))); do
    for s in $SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
        --skew "$s" --row-bytes "$RB" --cores "$CORES" --exec-mem "$MEM" \
        --partitions "$P" --tag "$TAG" --rep "$rep" 2>&1 | grep -E '^\[run\]|FAILED' || true
    done
  done

  if [ "$CLEANUP" = "1" ]; then
    echo "--- cleanup row_bytes=${RB} (다음 배치 공간 확보)"
    for s in $SKEWS; do
      rm -rf "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}"
    done
  else
    echo "--- keep datasets (CLEANUP=0)"
  fi
  df -h "$SPARK_SKEW_DATA" | tail -1 | awk '{print "  disk: "$3" used, "$4" free"}'
  echo
done

echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
echo
echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s2.py   (s1 의 rb=256 을 합쳐서 비교)"
