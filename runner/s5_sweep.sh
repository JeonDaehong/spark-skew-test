#!/usr/bin/env bash
# ============================================================================
# S5/S6 — cliff 을 커널이 만드는가?  (명제 P5)
#
# 지금까지의 문제
# --------------
# S1·S2·S4 에서 커널 신호가 계속 평탄했다. PSI io.full 4.6~5.6s, Dirty 최고치도
# skew 와 무관하게 일정. 이유는 단순하다:
#
#   61GB RAM -> dirty_background 10% = 6.1GB, dirty_ratio 20% = 12.2GB
#   실제 spill 최대                            = 1.24GB
#   => writeback 스로틀링 근처도 못 갔다.
#
# 그래서 "커널은 개입하지 않는다"가 결론인지, "이 규모에서 안 닿았을 뿐"인지
# 구분이 안 된다. 임계를 spill 양 아래로 끌어내려서 판별한다.
#
# 설계 (진짜 개입 실험)
# --------------------
# Spark 설정은 전부 고정하고 **커널 노브 하나만** 바꾼다.
#
#   dirty_ratio/background   임계(61GB 기준)      spill 1.24GB 대비
#     20 / 10  (기본)        12.2GB / 6.1GB       한참 아래  -> 무반응 예상
#      8 /  4                 4.9GB / 2.4GB       아직 아래
#      2 /  1                 1.2GB / 0.6GB       넘어섬     -> 발동해야 함
#
# 판정
# ----
#   dirty_ratio 를 낮췄는데 task 시간이 그대로   -> 커널은 이 규모에서 무관. P5 거짓.
#   낮출수록 느려지고 PSI/D-state 가 오른다      -> 경로가 실재. 그 다음 질문은
#                                                  "기본 설정에서도 닿는 조건이 있는가"
#
# 주의: sysctl 을 건드리므로 끝나면 반드시 원복한다 (trap).
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
SKEWS=${SKEWS:-"8 16 32 64"}
# "dirty_ratio:dirty_background_ratio" 쌍
RATIOS=${RATIOS:-"20:10 8:4 2:1"}
TAG=${TAG:-s5}

ORIG_DR=$(cat /proc/sys/vm/dirty_ratio)
ORIG_DBR=$(cat /proc/sys/vm/dirty_background_ratio)
restore() {
  echo "[s5] sysctl 원복: dirty_ratio=$ORIG_DR dirty_background_ratio=$ORIG_DBR"
  sudo sysctl -q -w vm.dirty_ratio="$ORIG_DR" vm.dirty_background_ratio="$ORIG_DBR" || true
}
trap restore EXIT INT TERM

echo "=== S5/S6 sweep (P5: cliff 을 커널이 만드는가) ==="
echo "  total=${GB}GiB rb=${RB} p=${P} cores=${CORES} mem=${MEM}"
echo "  skews: ${SKEWS}   dirty_ratio 쌍: ${RATIOS}   reps: ${REPS}"
echo "  원래 sysctl: dirty_ratio=$ORIG_DR background=$ORIG_DBR"
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
for pair in $RATIOS; do
  DR=${pair%%:*}; DBR=${pair##*:}
  echo "### vm.dirty_ratio=$DR  vm.dirty_background_ratio=$DBR"
  sudo sysctl -q -w vm.dirty_ratio="$DR" vm.dirty_background_ratio="$DBR"
  # 커널이 계산한 실제 임계(페이지 -> MiB). sysctl 의 % 가 아니라 이 값이 실효 임계다.
  awk '/nr_dirty_threshold/ {printf "  실효 임계      : %.0f MiB\n", $2*4/1024}
       /nr_dirty_background_threshold/ {printf "  실효 background: %.0f MiB\n", $2*4/1024}' /proc/vmstat

  # rep 을 바깥 루프에: 시간 순서 효과가 skew 와 교락되지 않게 (S1~S4 와 동일)
  for rep in $(seq 0 $((REPS-1))); do
    for s in $SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
        --workload sort --skew "$s" --row-bytes "$RB" \
        --cores "$CORES" --exec-mem "$MEM" --partitions "$P" \
        --tag "$TAG" --rep "$rep" \
        2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
    done
  done
  echo
done

echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s5.py --tag $TAG"
