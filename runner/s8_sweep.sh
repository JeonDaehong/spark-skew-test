#!/usr/bin/env bash
# ============================================================================
# S8 — 일반화 검증: 핵심 결론 3개가 sort 밖에서도 성립하는가?
#
# 왜 하는가
# ---------
# S1·S2·S3 의 결론이 **전부 sort 워크로드에서 나왔다.** join 은 S4(AQE)에서만
# 썼고 집계는 한 번도 안 돌렸다. 지금 블로그 초안의 한계 항목에도 그렇게 적혀 있다:
#
#   "정렬 작업 중심이다. 조인은 AQE 실험에서만 썼고, 집계는 안 해봤다."
#
# 그런데 우리가 주장하는 메커니즘(UnsafeExternalSorter 가 실행 풀을 채우다 넘친다)은
# **sort 전용이 아니어야 한다.** shuffle 후 한 파티션을 메모리에 쌓는 연산이면
# 다 같은 구조다. 그게 맞는지 확인하지 않으면 "sort 에서만 봤다"로 남는다.
#
# 검증할 결론 3개
# ---------------
#   A. 계단은 실행 메모리 풀 포화가 만든다        (S1/P1)
#   B. 첫 spill = 0.757 x pool / N  — 1/N 법칙    (S3)
#   C. 비용은 바이트가 아니라 레코드로 지불된다   (S2/P2)
#
# 워크로드 4종
# ------------
#   sort   대조군. 기존 결론이 나온 바로 그 워크로드
#   join   SortMergeJoin. hot key 양쪽을 정렬해서 맞춘다
#   agg    collect_list — 비대수적이라 map-side combine 으로 안 접힌다
#   count  **음성 대조군.** partial aggregation 이 skew 를 map 단계에서 흡수해야 한다.
#          여기서까지 계단이 보이면 우리 해석이 틀린 것이다.
#
# count 를 넣은 이유가 중요하다. "어떤 워크로드든 skew 면 느려진다"는 동어반복이고,
# **흡수되는 워크로드가 실제로 흡수되는 것을 보여야** 메커니즘 주장이 선다.
#
# 판정
# ----
#   A: sort/join/agg 에서 peak_exec_mem 이 같은 천장(~712 MiB)에 닿고
#      count 는 안 닿는다                          -> 일반화 성립
#   B: 첫 spill x N 이 워크로드마다 상수            -> 1/N 은 sorter 의 성질
#      워크로드마다 계수가 다르다                    -> 0.757 은 sort 고유
#   C: 바이트가 균일한데(record-skew) 시간이 오른다 -> 레코드 지배가 일반적
#
# 주의
# ----
#   agg 는 hot key 하나에 collect_list 가 수백만 원소 배열을 만든다. skew 32 에서
#   OOM 이 날 수 있다. 그건 실패가 아니라 결과다 — run_one 이 error 로 기록한다.
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

GB=${GB:-8}
RB=${RB:-256}
P=${P:-200}
MEM=${MEM:-1500m}
CORES=${CORES:-4}
TAG=${TAG:-s8}

# A. cliff 재현
A_WORKLOADS=${A_WORKLOADS:-"sort join agg count"}
A_SKEWS=${A_SKEWS:-"8 16 24 32"}
A_REPS=${A_REPS:-3}

# B. 1/N 법칙 재현.  skew 16 은 core 1~6 **전부** 평탄구간이다 (S3 확인):
#    첫 spill 이 c1 544 / c2 272 / c4 136 / c6 85 MiB 로 나왔고 아직 cliff 전이다.
#    count 는 spill 자체가 안 나므로 제외한다.
B_WORKLOADS=${B_WORKLOADS:-"sort join agg"}
B_CORES=${B_CORES:-"1 2 4 6"}
B_SKEW=${B_SKEW:-16}
B_REPS=${B_REPS:-2}

# C. 레코드 지배 재현. hot=좁은 행 / cold=넓은 행 -> 바이트는 거의 균일한데 레코드만 치우친다
C_WORKLOADS=${C_WORKLOADS:-"sort join agg"}
C_REC_SKEWS=${C_REC_SKEWS:-"1 8 32"}
C_HOT_RB=${C_HOT_RB:-64}
C_COLD_RB=${C_COLD_RB:-2048}
C_REPS=${C_REPS:-3}

na=$(( $(echo "$A_WORKLOADS" | wc -w) * $(echo "$A_SKEWS" | wc -w) * A_REPS ))
nb=$(( $(echo "$B_WORKLOADS" | wc -w) * $(echo "$B_CORES" | wc -w) * B_REPS ))
nc=$(( $(echo "$C_WORKLOADS" | wc -w) * $(echo "$C_REC_SKEWS" | wc -w) * C_REPS ))

echo "=== S8 sweep (일반화: 핵심 결론이 sort 밖에서도 성립하는가) ==="
echo "  A cliff      : ${A_WORKLOADS} x skew{${A_SKEWS}} x ${A_REPS}  = ${na} run"
echo "  B 1/N 법칙   : ${B_WORKLOADS} x cores{${B_CORES}} @skew${B_SKEW} x ${B_REPS} = ${nb} run"
echo "  C 레코드지배 : ${C_WORKLOADS} x R{${C_REC_SKEWS}} x ${C_REPS}  = ${nc} run"
echo "  합계 $((na+nb+nc)) run  ·  풀 720MB 고정 · AQE off · p=${P}"
echo

start=$(date +%s)

# ---------------------------------------------------------------- datasets
echo "### 데이터셋 (byte-skew)"
# A 와 B 가 같은 데이터셋을 쓴다. B_SKEW 가 A_SKEWS 에 없으면 따로 만든다.
ALL_BYTE_SKEWS=$(printf '%s\n' $A_SKEWS $B_SKEW | sort -n -u | tr '\n' ' ')
for s in $ALL_BYTE_SKEWS; do
  d="$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}"
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): skew=$s"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$GB" --row-bytes "$RB" \
      --skew "$s" --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
done

echo "### 데이터셋 (record-skew)"
for r in $C_REC_SKEWS; do
  d="$SPARK_SKEW_DATA/rec${r}_rb${C_HOT_RB}_cb${C_COLD_RB}_p${P}_g${GB}"
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): R=$r"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$GB" --skew-mode record \
      --record-skew "$r" --row-bytes "$C_HOT_RB" --cold-row-bytes "$C_COLD_RB" \
      --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
done
echo

# ---------------------------------------------------------------- A
echo "### A. cliff 재현 (워크로드 x skew)"
# rep 을 바깥 루프에 — 시간 순서 효과가 워크로드와 교락되지 않게 (전 스테이지 공통)
for rep in $(seq 0 $((A_REPS-1))); do
  for w in $A_WORKLOADS; do
    for s in $A_SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
        --workload "$w" --skew-mode byte \
        --skew "$s" --row-bytes "$RB" \
        --cores "$CORES" --exec-mem "$MEM" --partitions "$P" \
        --aqe false --tag "$TAG" --rep "$rep" \
        2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
    done
  done
done
echo

# ---------------------------------------------------------------- B
echo "### B. 1/N 법칙 재현 (워크로드 x cores @ skew ${B_SKEW})"
for rep in $(seq 0 $((B_REPS-1))); do
  for w in $B_WORKLOADS; do
    for c in $B_CORES; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/skew${B_SKEW}_rb${RB}_p${P}_g${GB}" \
        --workload "$w" --skew-mode byte \
        --skew "$B_SKEW" --row-bytes "$RB" \
        --cores "$c" --exec-mem "$MEM" --partitions "$P" \
        --aqe false --tag "$TAG" --rep "$rep" \
        2>&1 | grep -E '^\[run\]|FAILED|ABORT' || true
    done
  done
done
echo

# ---------------------------------------------------------------- C
echo "### C. 레코드 지배 재현 (워크로드 x record-skew)"
for rep in $(seq 0 $((C_REPS-1))); do
  for w in $C_WORKLOADS; do
    for r in $C_REC_SKEWS; do
      python runner/run_one.py \
        --input "$SPARK_SKEW_DATA/rec${r}_rb${C_HOT_RB}_cb${C_COLD_RB}_p${P}_g${GB}" \
        --workload "$w" --skew-mode record --record-skew "$r" \
        --skew 1 --row-bytes "$C_HOT_RB" \
        --cores "$CORES" --exec-mem "$MEM" --partitions "$P" \
        --aqe false --tag "$TAG" --rep "$rep" \
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
echo "다음: python analysis/plot_s8.py --tag $TAG"
