#!/usr/bin/env bash
# ============================================================================
# S3 — executor 당 core 수가 skew 내성을 정하는가?  (명제 P4)
#
# 널리 퍼진 예측
# -------------
# Spark 의 unified memory manager 는 실행 메모리 풀을 executor 안의 active task
# N 개가 나눠 갖는다 (보장 하한 1/2N ~ 1/N). 그래서 튜닝 문서와 블로그들은 이렇게 쓴다:
#
#   "core 를 늘리면 task 당 메모리가 줄어 spill 이 더 일찍 터지고 skew 가 악화된다"
#
# 예측까지 이미 문서화돼 있다 (docs/04 문헌 재확인 참조). 측정만 없다.
#
# 그런데 우리는 S1 에서 반대 방향의 사실을 실측했다
# -------------------------------------------------
# 1g 풀(434MB)에서 peak execution memory 가 310MB 까지 올라가고도 spill 이 안 났다.
# 1/2N ~ 1/N 은 **보장 하한**이고, 다른 task 가 놀면 한 task 가 **풀 전체**를 가져간다.
#
# skew 가 크면 hot task 는 다른 task 들이 다 끝난 뒤까지 혼자 남아 돈다.
# 그렇다면 core 수를 늘려도 hot task 는 여전히 풀 전체를 쓸 수 있고,
# **P4 는 거짓일 수 있다.**
#
# 설계
# ----
#   실행 메모리 풀을 고정(exec-mem 1500m = 720MB)하고 동시 task 수만 바꾼다.
#   비교 지표는 raw 시간이 아니라 **cliff 의 위치**다 —
#   core 가 늘수록 계단이 더 낮은 skew 로 당겨오는가?
#
#   cores  1 · 2 · 4 · 6      (총 메모리 풀은 동일)
#   skew   8 · 12 · 16 · 20 · 24 · 32   (S2 와 같은 촘촘한 격자)
#   reps   2   (peak_exec_mem 은 결정론적이라 산포가 거의 없다 — S1 에서 머신이 달라도 동일)
#
# 판정
# ----
#   core 가 늘수록 peak_exec_mem 포화가 더 낮은 skew 에서 일어남  -> P4 참
#   포화 지점이 core 수와 무관                                    -> P4 거짓
#                                                                   (= "한 task 가 풀 전체를
#                                                                      가져간다"가 지배)
#
# 주의: cores=1 은 병렬성이 없어 wall 이 4배 가까이 길다. 시간 비교가 아니라
#       cliff 위치 비교라는 점을 잊지 말 것.
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
REPS=${REPS:-2}
SKEWS=${SKEWS:-"8 12 16 20 24 32"}
CORES_LIST=${CORES_LIST:-"1 2 4 6"}
TAG=${TAG:-s3}

echo "=== S3 sweep (P4: cores-per-executor 가 skew 내성을 정하는가) ==="
echo "  total=${GB}GiB rb=${RB} p=${P} mem=${MEM} (풀 720MB 고정)"
echo "  cores: ${CORES_LIST}   skews: ${SKEWS}   reps: ${REPS}"
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
# rep 을 바깥 루프에 — 시간 순서 효과가 core 수와 교락되지 않게 (전 스테이지 공통)
for rep in $(seq 0 $((REPS-1))); do
  for c in $CORES_LIST; do
    for s in $SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
        --workload sort --skew "$s" --row-bytes "$RB" \
        --cores "$c" --exec-mem "$MEM" --partitions "$P" \
        --tag "$TAG" --rep "$rep" \
        2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
    done
  done
done

echo
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s3.py --tag $TAG"
