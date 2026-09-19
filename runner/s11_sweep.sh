#!/usr/bin/env bash
# ============================================================================
# S11 — 실무 편: skew 를 실제로 어떻게 만나고 어떻게 막는가
#
# 왜 하는가
# ---------
# 블로그 앞부분에 "skew 는 어디서 생기고 AQE 는 어디까지 해주는가"를 넣으려는데,
# 기존 실험에 **없는 것**이 셋이었다. 측정 없이 일반론으로 쓰면 이 글의 축
# ("네 개 예상 중 셋이 틀렸다")과 결이 안 맞는다. 그래서 잰다.
#
#   A. broadcast join 은 어디서 어떻게 터지는가
#   B. AQE 가 손대지 않는 구간이 있는가 (hot < 256MB)
#   C. salting 은 실제로 얼마나 버는가
#
# 이미 가지고 있는 것 (다시 안 잰다)
# ---------------------------------
#   - AQE 는 byte-skew 를 3/3 쪼개고 record-skew 를 0/6 놓친다 (S4/S4b)
#   - record R=64 는 hot 376.9 MiB 로 **256MB 임계를 넘는데도** 안 쪼갠다
#     -> 두 번째 조건(중앙값의 5배)에서 걸러진다
#   - AQE 가 분할이 아니라 **병합**만 하는 경우도 있다 (record R=1: 135.6 -> 203.4 MiB)
#
# ----------------------------------------------------------------------------
# A. broadcast join 의 경계
#
#   dim 을 키 개수로 키운다. fact 의 키 범위를 넘는 행은 매칭되지 않는데
#   그게 현실이다 — 디멘션은 보통 참조되는 것보다 크다.
#   broadcast 는 힌트로 **강제**한다 (세션의 autoBroadcastJoinThreshold=-1 보다 우선).
#
#   예측: dim 이 driver 힙에 비해 커지는 순간 OOM. 그 지점과 증상을 기록한다.
#   대조: 같은 dim 을 SortMergeJoin 으로 돌리면 (느려도) 끝난다.
#
# B. AQE 가 손대지 않는 구간
#
#   AQE 의 판정은 두 조건의 **AND** 다 (OptimizeSkewedJoin.scala):
#       size > 256MB  AND  size > median x 5
#   총 데이터를 줄여 hot 파티션이 256MB 를 못 넘게 만들면, skew 가 아무리 커도
#   첫 조건에서 걸린다. **AQE 를 켜도 아무 일이 안 일어나야 한다.**
#   이건 "AQE 있으니 괜찮다"가 어디서 깨지는지를 보여주는 실무 경계다.
#
# C. salting
#
#   hot key 에 무작위 salt 를 붙여 여러 파티션으로 흩는다. dim 은 salt 배로
#   복제해야 매칭이 유지된다 — **그 복제가 이 기법의 대가다.**
#   세 팔을 같은 조건에서 나란히 잰다: 무처리 / AQE / salting.
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

# --- 완료 표식 (env/ec2-sync.sh wait 가 이걸 본다) -------------------------
_DONE_MARK="/home/ubuntu/.$(basename "$0").done"
rm -f "$_DONE_MARK"
trap 'echo "exit=$?" > "$_DONE_MARK"' EXIT

RB=${RB:-256}
P=${P:-200}
MEM=${MEM:-1500m}
CORES=${CORES:-4}
TAG=${TAG:-s11}

# A
A_GB=${A_GB:-4}
A_SKEW=${A_SKEW:-8}
A_DIM_KEYS=${A_DIM_KEYS:-"10001 500000 1500000 3000000 5000000"}
A_DIM_RB=${A_DIM_RB:-200}
A_REPS=${A_REPS:-2}

# B — hot 파티션이 256MB 를 못 넘게 총량을 줄인다
B_GB=${B_GB:-1}
B_SKEWS=${B_SKEWS:-"1 8 20 50"}
B_REPS=${B_REPS:-2}

# C
C_GB=${C_GB:-4}
C_SKEWS=${C_SKEWS:-"8 32"}
C_SALT=${C_SALT:-16}
C_REPS=${C_REPS:-3}

gen () {  # $1=gb  $2=skew
  local d="$SPARK_SKEW_DATA/skew${2}_rb${RB}_p${P}_g${1}"
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): gb=$1 skew=$2"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$1" --row-bytes "$RB" \
      --skew "$2" --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
}

run () {  # $1=workload $2=label $3=gb $4=skew $5=aqe $6=extra...
  local w=$1 lbl=$2 gb=$3 sk=$4 aqe=$5; shift 5
  # shellcheck disable=SC2086
  python runner/run_one.py \
    --input "$SPARK_SKEW_DATA/skew${sk}_rb${RB}_p${P}_g${gb}" \
    --workload "$w" --label "$lbl" \
    --skew "$sk" --row-bytes "$RB" \
    --cores "$CORES" --exec-mem "$MEM" --partitions "$P" \
    --aqe "$aqe" --tag "$TAG" "$@" \
    2>&1 | grep -E '^\[run\]|ABORT' || true
}

na=$(( $(echo "$A_DIM_KEYS" | wc -w) * A_REPS * 2 ))
nb=$(( $(echo "$B_SKEWS" | wc -w) * B_REPS * 2 ))
nc=$(( $(echo "$C_SKEWS" | wc -w) * C_REPS * 3 ))
echo "=== S11 sweep (실무 편) ==="
echo "  A broadcast 경계 : dim{${A_DIM_KEYS}} x ${A_REPS}rep x (bc/smj) = ${na} run"
echo "  B AQE 사각 구간  : ${B_GB}GiB skew{${B_SKEWS}} x ${B_REPS}rep x (on/off) = ${nb} run"
echo "  C salting        : ${C_GB}GiB skew{${C_SKEWS}} x ${C_REPS}rep x 3팔 = ${nc} run"
echo "  합계 $((na+nb+nc)) run"
echo

echo "### 데이터셋"
gen "$A_GB" "$A_SKEW"
for s in $B_SKEWS; do gen "$B_GB" "$s"; done
for s in $C_SKEWS; do gen "$C_GB" "$s"; done
echo

start=$(date +%s)

# ---------------------------------------------------------------- A
echo "### A. broadcast join 은 어디서 터지는가"
for rep in $(seq 0 $((A_REPS-1))); do
  for dk in $A_DIM_KEYS; do
    mb=$(( dk * A_DIM_RB / 1048576 ))
    echo "  -- dim ${dk} keys (~${mb} MiB)"
    run join_bc "bc${mb}" "$A_GB" "$A_SKEW" false \
        --rep "$rep" --dim-keys "$dk" --dim-row-bytes "$A_DIM_RB"
    # 대조군: 같은 dim 을 SortMergeJoin 으로. 느려도 끝나야 한다.
    run join "smj${mb}" "$A_GB" "$A_SKEW" false \
        --rep "$rep" --dim-keys "$dk" --dim-row-bytes "$A_DIM_RB"
  done
done
echo

# ---------------------------------------------------------------- B
echo "### B. AQE 가 손대지 않는 구간 (hot < 256MB)"
for rep in $(seq 0 $((B_REPS-1))); do
  for s in $B_SKEWS; do
    for aqe in false true; do
      run join "aqe${aqe}" "$B_GB" "$s" "$aqe" --rep "$rep"
    done
  done
done
echo

# ---------------------------------------------------------------- C
echo "### C. salting vs AQE vs 무처리"
for rep in $(seq 0 $((C_REPS-1))); do
  for s in $C_SKEWS; do
    run join      none  "$C_GB" "$s" false --rep "$rep"
    run join      aqe   "$C_GB" "$s" true  --rep "$rep"
    run join_salt salt  "$C_GB" "$s" false --rep "$rep" --salt "$C_SALT"
  done
done

echo
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
df -h "$SPARK_SKEW_DATA" | tail -1 | awk '{print "  disk: "$3" used, "$4" free"}'

echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
echo
echo "다음: python analysis/plot_s11.py --tag $TAG"
