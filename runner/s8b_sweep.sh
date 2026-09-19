#!/usr/bin/env bash
# ============================================================================
# S8b — 키별 연산에서 계단은 언제 생기고 언제 안 생기는가?
#
# S8 이 남긴 구멍
# ---------------
# S8 의 agg 팔은 `collect_list(substring(payload, 1, 8))` 이었다. 256B 행을 8B 로
# 줄여 모으니 shuffle 이 32 배 작아졌고 hot 파티션이 1054 MiB 가 아니라 52 MiB 였다.
# 실행 풀(720 MiB) 근처도 못 갔다. "집계는 계단이 없다"가 아니라 시험이 물렀다.
#
# 스모크에서 알아낸 것 (이 설계의 근거)
# ------------------------------------
# payload 를 안 자른 `agg_wide` 로 다시 해봤더니 **이 질문 자체가 잘못 놓여 있었다.**
#
#   base(기본 폴백 임계 128) : stage1(map-side 부분집계)에서 OOM. reduce 근처도 못 감
#   fb0(즉시 폴백)           : stage1·stage2 전부 통과, 658 MiB spill 까지 하고 그 뒤 사망
#
# 이유: hot key 의 **출력 한 행**이 4.6M x 248B = 1.15 GB 다. 풀(720 MiB)을 넘기려면
# 출력이 반드시 힙보다 커진다. "풀을 넘었다"와 "출력이 너무 크다"를 원리적으로
# 분리할 수 없다. collect_list 로는 계단을 잴 수 없다.
#
# 그래서 세 갈래로 나눠서 묻는다
# ------------------------------
#   sort    기준선. 같은 세션·같은 인스턴스에서 나란히 잰다
#   window  row_number() over (partition by key order by payload)
#           현장에서 skew 로 제일 자주 터지는 "키별 중복제거 / top-N" 패턴.
#           hot 파티션이 축약 없이 reduce 로 오고(계단 조건 충족), 출력은 입력 행마다
#           한 행이라 경계가 있다. WindowExec 는 UnsafeExternalSorter 를 쓴다.
#           => 계단이 나와야 하고 spill 로 살아남아야 한다.
#   agg_wide  collect_list(payload). **실패하는 것이 결과다.**
#           spill 없이 죽는지, 폴백을 강제하면 spill 하고도 죽는지를 기록한다.
#
# 판정
# ----
#   window 가 sort 와 같은 천장(712 MiB)에 닿고 spill 하며 산다
#       -> 계단은 "키별로 큰 파티션을 메모리에 쌓는 연산" 일반의 성질이다
#   agg_wide base 가 spill 0 인 채로 죽는다
#       -> 축약 불가능한 집계에는 spill 이라는 안전밸브가 걸리지 않는다
#   agg_wide fb0 가 spill 하고도 죽는다
#       -> 안전밸브를 강제로 달아도 출력 크기 때문에 못 산다 (설계상 한계)
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
RB=${RB:-256}
P=${P:-200}
MEM=${MEM:-1500m}
CORES=${CORES:-4}
SKEWS=${SKEWS:-"8 16 24 32"}
REPS=${REPS:-3}
AGG_REPS=${AGG_REPS:-1}          # 실패를 특성화하는 팔이라 1 회면 충분하다
TAG=${TAG:-s8b}

FB=spark.sql.objectHashAggregate.sortBased.fallbackThreshold

ns=$(echo "$SKEWS" | wc -w)
echo "=== S8b sweep (키별 연산의 계단과 안전밸브) ==="
echo "  skews: ${SKEWS}  cores=${CORES} mem=${MEM} (풀 720MB) p=${P}"
echo "  window ${REPS}rep + sort ${REPS}rep + agg_wide(base/fb0) ${AGG_REPS}rep"
echo "  총 $(( ns * (REPS*2 + AGG_REPS*2) )) run"
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

one () {   # $1=workload  $2=label  $3=skew  $4=rep  $5=extra
  # shellcheck disable=SC2086
  python runner/run_one.py \
    --input "$SPARK_SKEW_DATA/skew${3}_rb${RB}_p${P}_g${GB}" \
    --workload "$1" --label "$2" $5 \
    --skew "$3" --row-bytes "$RB" \
    --cores "$CORES" --exec-mem "$MEM" --partitions "$P" \
    --aqe false --tag "$TAG" --rep "$4" \
    2>&1 | grep -E '^\[run\]|ABORT' || true
}

# rep 을 바깥 루프에 — 시간 순서 효과가 워크로드와 교락되지 않게 (전 스테이지 공통)
echo "### A. window vs sort (계단이 일반화되는가)"
for rep in $(seq 0 $((REPS-1))); do
  for s in $SKEWS; do
    one window "" "$s" "$rep" ""
    one sort   ref "$s" "$rep" ""
  done
done
echo

echo "### B. agg_wide — 실패를 특성화한다"
for rep in $(seq 0 $((AGG_REPS-1))); do
  for s in $SKEWS; do
    one agg_wide base "$s" "$rep" ""
    one agg_wide fb0  "$s" "$rep" "--extra-conf ${FB}=0"
  done
done

echo
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
df -h "$SPARK_SKEW_DATA" | tail -1 | awk '{print "  disk: "$3" used, "$4" free"}'

echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s8b.py --tag $TAG"
