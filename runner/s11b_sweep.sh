#!/usr/bin/env bash
# ============================================================================
# S11b — 나머지 해결책들: 실제로 얼마나 버는가
#
# S11 에서 broadcast / AQE / salting 셋을 쟀다. 그런데 그게 전부가 아니다.
# 여기서 나머지를 전부 잰다. **"더 있으면 그것들도 다 테스트해서 넣어야지"**
#
#   D. AQE 임계값 낮추기   — S11-B 에서 찾은 사각지대를 **이걸로 메울 수 있는가**
#   E. hot key 분리 처리   — AQE 도 salting 도 못 쓸 때의 수동 해법
#   F. 파티션 수 늘리기    — 제일 먼저 시도하는 것. **음성 대조군**
#   G. NULL key            — 실무 skew 의 상당수. Spark 가 알아서 해주는가?
#
# ----------------------------------------------------------------------------
# D. AQE 임계값 낮추기
#
#   S11-B 에서 확인: 1 GiB 데이터의 hot 파티션은 86 MiB 라 256MB 임계에 못 미쳐
#   AQE 가 켜져 있어도 **아무것도 안 한다.** 판정이 두 조건의 AND 이기 때문이다.
#       size > skewedPartitionThresholdInBytes (256MB)
#         AND size > median x skewedPartitionFactor (5)
#   첫 조건을 32MB 로 낮추면 AQE 가 손을 대야 한다. 대면 얼마나 버는가.
#
# E. hot key 분리 처리 (isolation)
#
#   hot key 만 떼어 broadcast join, 나머지는 SMJ, 둘을 union.
#   salting 과 달리 **dim 을 복제하지 않는다** — hot 쪽 dim 은 한 행뿐이다.
#   salting 의 대가(dim x salt)를 안 치르고 같은 효과가 나는지 본다.
#
# F. 파티션 수 늘리기 (음성 대조군)
#
#   skew 를 만나면 제일 먼저 `spark.sql.shuffle.partitions` 를 올린다.
#   그런데 **키 하나에 몰린 거라면 200 개든 2000 개든 그 키는 여전히 한 파티션**이다.
#   안 나아져야 정상이다. 나아지면 내 해석이 틀린 것이다.
#
# G. NULL key
#
#   해시 파티셔닝에서 NULL 은 전부 같은 파티션으로 간다. hot key 와 같은 구조다.
#   그런데 inner join 이면 Spark 가 조인 키에 isnotnull 을 자동으로 끼워넣어
#   (InferFiltersFromConstraints) NULL 을 셔플 전에 털어낼 수 있다.
#   left outer 는 NULL 행을 버릴 수 없으니 그 최적화가 막힌다.
#   => inner 는 공짜, outer 는 직격. 그 차이를 잰다.
# ============================================================================
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

_DONE_MARK="/home/ubuntu/.$(basename "$0").done"
rm -f "$_DONE_MARK"
trap 'echo "exit=$?" > "$_DONE_MARK"' EXIT

RB=${RB:-256}
P=${P:-200}
MEM=${MEM:-1500m}
CORES=${CORES:-4}
TAG=${TAG:-s11b}

D_GB=${D_GB:-1}
D_SKEWS=${D_SKEWS:-"20 50"}
D_LOW=${D_LOW:-33554432}          # 32 MiB
D_REPS=${D_REPS:-3}

E_GB=${E_GB:-4}
E_SKEWS=${E_SKEWS:-"8 32"}
E_SALT=${E_SALT:-16}
E_REPS=${E_REPS:-3}

F_GB=${F_GB:-4}
F_SKEW=${F_SKEW:-32}
F_PARTS=${F_PARTS:-"200 800 2000"}
F_REPS=${F_REPS:-2}

G_GB=${G_GB:-4}
G_NULL=${G_NULL:-0.30}
G_REPS=${G_REPS:-3}

THR=spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes

gen () {  # $1=gb $2=skew [$3=null_fraction]
  local nf=${3:-0}
  local d
  if [ "$nf" = "0" ]; then
    d="$SPARK_SKEW_DATA/skew${2}_rb${RB}_p${P}_g${1}"
  else
    d="$SPARK_SKEW_DATA/null${nf}_skew${2}_rb${RB}_p${P}_g${1}"
  fi
  if [ -d "$d" ] && [ -f "$d/_gen_meta.json" ]; then
    echo "  skip (exists): $(basename "$d")"
  else
    rm -rf "$d"
    python gen/make_skewed.py --total-gb "$1" --row-bytes "$RB" \
      --skew "$2" --partitions "$P" --null-fraction "$nf" --out "$d" \
      2>&1 | grep -E '^\[gen\]' || true
  fi
  echo "$d"
}

run () {  # $1=input $2=workload $3=label $4=skew $5=aqe $6=parts $7...=extra
  local inp=$1 w=$2 lbl=$3 sk=$4 aqe=$5 parts=$6; shift 6
  # shellcheck disable=SC2086
  python runner/run_one.py --input "$inp" \
    --workload "$w" --label "$lbl" --skew "$sk" --row-bytes "$RB" \
    --cores "$CORES" --exec-mem "$MEM" --partitions "$parts" \
    --aqe "$aqe" --tag "$TAG" "$@" \
    2>&1 | grep -E '^\[run\]|ABORT' || true
}

echo "=== S11b sweep (나머지 해결책 전부) ==="
start=$(date +%s)

echo "### 데이터셋"
for s in $D_SKEWS; do gen "$D_GB" "$s" >/dev/null; done
for s in $E_SKEWS; do gen "$E_GB" "$s" >/dev/null; done
gen "$F_GB" "$F_SKEW" >/dev/null
G_DIR=$(gen "$G_GB" 1 "$G_NULL" | tail -1)
echo "  null 데이터셋: $G_DIR"
echo

# ---------------------------------------------------------------- D
echo "### D. AQE 임계값을 32MiB 로 낮추면 사각지대가 메워지는가"
for rep in $(seq 0 $((D_REPS-1))); do
  for s in $D_SKEWS; do
    inp="$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${D_GB}"
    run "$inp" join aqe_off  "$s" false "$P" --rep "$rep"
    run "$inp" join aqe_256m "$s" true  "$P" --rep "$rep"
    run "$inp" join aqe_32m  "$s" true  "$P" --rep "$rep" --extra-conf "${THR}=${D_LOW}"
  done
done
echo

# ---------------------------------------------------------------- E
echo "### E. hot key 분리 처리 vs salting vs AQE vs 무처리"
for rep in $(seq 0 $((E_REPS-1))); do
  for s in $E_SKEWS; do
    inp="$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${E_GB}"
    run "$inp" join         none    "$s" false "$P" --rep "$rep"
    run "$inp" join         aqe     "$s" true  "$P" --rep "$rep"
    run "$inp" join_salt    salt    "$s" false "$P" --rep "$rep" --salt "$E_SALT"
    run "$inp" join_isolate isolate "$s" false "$P" --rep "$rep"
  done
done
echo

# ---------------------------------------------------------------- F
echo "### F. 파티션 수를 늘리면 나아지는가 (음성 대조군)"
for rep in $(seq 0 $((F_REPS-1))); do
  for pp in $F_PARTS; do
    inp="$SPARK_SKEW_DATA/skew${F_SKEW}_rb${RB}_p${P}_g${F_GB}"
    run "$inp" join "p${pp}" "$F_SKEW" false "$pp" --rep "$rep"
  done
done
echo

# ---------------------------------------------------------------- G
echo "### G. NULL key — Spark 가 알아서 해주는가"
for rep in $(seq 0 $((G_REPS-1))); do
  run "$G_DIR" join      inner "1" false "$P" --rep "$rep"
  run "$G_DIR" join_left outer "1" false "$P" --rep "$rep"
done

echo
echo "  elapsed: $(( ($(date +%s) - start) / 60 )) min"
df -h "$SPARK_SKEW_DATA" | tail -1 | awk '{print "  disk: "$3" used, "$4" free"}'

echo "### parse"
python parse/parse_eventlog.py --tag "$TAG"
