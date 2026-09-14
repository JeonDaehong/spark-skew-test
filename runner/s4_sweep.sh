#!/usr/bin/env bash
# ============================================================================
# S4 — AQE 는 record-skew 를 보는가?  (명제 P3)
#
# 배경
# ----
# S2: 비용은 바이트가 아니라 레코드로 지불된다 (1741 ns/record vs 4.95 ns/byte).
# 그런데 AQE 의 skew 판정은 ShufflePartitionsUtil 에서 bytesByPartitionId 만 본다.
#   => 바이트로는 평범한데 레코드가 몰린 파티션은 감지되지 않으면서 실제로는 비싸다.
#
# 설계 (2군 비교)
# --------------
#   A. byte-skew   : 행 폭 고정, hot key 에 행을 몰아줌
#                    -> hot 파티션이 256MB & 5x median 조건을 넘김 -> AQE 발동해야 함
#                       (positive control. 이게 안 되면 B 의 '미발동'은 무의미하다)
#   B. record-skew : hot key 는 좁은 행(64B), cold key 는 넓은 행(2048B)
#                    -> byte skew <= 2, hot 파티션 ~81MB -> 두 조건 모두 미달
#                       -> AQE 가 구조적으로 발동 불가. 그런데 비용은?
#
# 각 군을 AQE off/on 으로 돌려서 "AQE 가 구제하는가"를 직접 비교한다.
#
# 워크로드는 join 이다. AQE 의 skewJoin 은 SortMergeJoin 에만 적용되므로
# sort 워크로드로는 검증 자체가 불가능하다.
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

GB=${GB:-8}
P=${P:-200}
CORES=${CORES:-4}
MEM=${MEM:-1500m}
REPS=${REPS:-3}                       # EC2 산포가 작아 3회로 충분 (IQR 0.02~1.1)
BYTE_SKEWS=${BYTE_SKEWS:-"1 8 16 32"}
REC_SKEWS=${REC_SKEWS:-"1 4 8 16 32"}
HOT_RB=${HOT_RB:-64}
COLD_RB=${COLD_RB:-2048}
RB=${RB:-256}                         # byte-skew 군의 행 폭
CLEANUP=${CLEANUP:-0}
TAG=${TAG:-s4}

echo "=== S4 sweep (P3: AQE 는 record-skew 를 보는가) ==="
echo "  A byte-skew  : rb=${RB}  skews: ${BYTE_SKEWS}"
echo "  B record-skew: hot=${HOT_RB}B cold=${COLD_RB}B  R: ${REC_SKEWS}"
echo "  AQE off/on x ${REPS} reps  ·  workload=join"
echo

start=$(date +%s)

# ---------- A. byte-skew ----------
echo "### A. byte-skew datasets"
for s in $BYTE_SKEWS; do
  d="$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}"
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): skew=$s"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$GB" --row-bytes "$RB" \
      --skew "$s" --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
done

echo "### A. runs"
for rep in $(seq 0 $((REPS-1))); do
  for aqe in false true; do
    for s in $BYTE_SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
        --workload join --skew-mode byte \
        --skew "$s" --row-bytes "$RB" --cores "$CORES" --exec-mem "$MEM" \
        --partitions "$P" --aqe "$aqe" --tag "$TAG" --rep "$rep" \
        2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
    done
  done
done

# ---------- B. record-skew ----------
echo
echo "### B. record-skew datasets"
for r in $REC_SKEWS; do
  d="$SPARK_SKEW_DATA/rec${r}_rb${HOT_RB}_cb${COLD_RB}_p${P}_g${GB}"
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): R=$r"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$GB" --skew-mode record \
      --record-skew "$r" --row-bytes "$HOT_RB" --cold-row-bytes "$COLD_RB" \
      --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
done

echo "### B. runs"
for rep in $(seq 0 $((REPS-1))); do
  for aqe in false true; do
    for r in $REC_SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/rec${r}_rb${HOT_RB}_cb${COLD_RB}_p${P}_g${GB}" \
        --workload join --skew-mode record --record-skew "$r" \
        --skew 1 --row-bytes "$HOT_RB" --cores "$CORES" --exec-mem "$MEM" \
        --partitions "$P" --aqe "$aqe" --tag "$TAG" --rep "$rep" \
        2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
    done
  done
done

echo
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
df -h "$SPARK_SKEW_DATA" | tail -1 | awk '{print "  disk: "$3" used, "$4" free"}'

echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s4.py --tag $TAG"
