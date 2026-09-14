#!/usr/bin/env bash
# ============================================================================
# S1 — cliff 탐색.  이 프로젝트 전체의 전제를 판정하는 실험.
#
#   "동일한 총 바이트에서 skew 만 키울 때, latency 는 어디서 꺾이는가?"
#
# 사이징 근거
# -----------
#   per-task execution pool = (exec_mem - 300MB) * spark.memory.fraction(0.6)
#   한 task 는 다른 task 가 놀면 풀 전체를 가져갈 수 있으므로 상한은 풀 전체다.
#   hot partition 의 in-memory footprint ~= shuffle_read_bytes * 1.3 (스모크 실측)
#
#   MEM=1500m -> pool = (1500-300)*0.6 = 720MB
#   GB=8, P=200 -> median partition = 41MB
#   spill 시작 지점 S ~= 720 / (41 * 1.3) ~= 13
#   => sweep(1..64) 한가운데에 cliff 이 오도록 설계됨.
#
# 결과 해석
# ---------
#   꺾임이 보이고 반복 IQR 보다 크다  -> 8주 계획 그대로 진행
#   완만한 무릎                        -> RQ4(커널) 축소, RQ2/RQ5/RQ6 로 재편
#   노이즈에 묻힘                      -> 환경 통제부터 다시 (B6)
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

GB=${GB:-8}
RB=${RB:-256}
P=${P:-200}
CORES=${CORES:-4}
MEM=${MEM:-1500m}
REPS=${REPS:-5}
SKEWS=${SKEWS:-"1 2 4 8 16 32 64"}
TAG=${TAG:-s1}

echo "=== S1 sweep ==="
echo "  total=${GB}GiB row_bytes=${RB} partitions=${P} cores=${CORES} mem=${MEM}"
echo "  skews: ${SKEWS}   reps: ${REPS}   tag: ${TAG}"
echo

# ---- 1. 데이터 생성 (있으면 건너뜀) ----
echo "### generate  ($(echo $SKEWS | wc -w) datasets x ${GB}GiB)"
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

# ---- 2. 실행 ----
# rep 을 바깥 루프에 둔다: skew 별로 몰아서 돌리면 시간 순서 효과(디스크 상태,
# 열 누적 등)가 skew 와 교락된다. rep 마다 전체 skew 를 한 바퀴 도는 편이 안전하다.
echo "### run  ($(echo $SKEWS | wc -w) skews x ${REPS} reps)"
start=$(date +%s)
for rep in $(seq 0 $((REPS-1))); do
  for s in $SKEWS; do
    python runner/run_one.py \
      --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
      --skew "$s" --row-bytes "$RB" --cores "$CORES" --exec-mem "$MEM" \
      --partitions "$P" --tag "$TAG" --rep "$rep" 2>&1 | grep -E '^\[run\]|FAILED' || true
  done
done
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
echo

# ---- 3. 파싱 ----
echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_cliff.py --tag $TAG"
