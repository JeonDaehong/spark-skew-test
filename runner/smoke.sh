#!/usr/bin/env bash
# 스모크 테스트: 1GiB 로 생성 -> 실행 -> 파싱 전 구간이 도는지 확인한다.
# 본 실험(S1) 전에 반드시 통과시킨다. 하네스가 깨진 채 1.5시간을 날리지 않기 위함.
set -euo pipefail
source ~/.spark-skew-env
cd "$(dirname "$0")/.."

GB=${GB:-1}
RB=${RB:-256}
P=${P:-200}
CORES=${CORES:-4}
MEM=${MEM:-1g}
SKEWS=${SKEWS:-"1 4 16 64"}

echo "### generate"
for s in $SKEWS; do
  d="$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}"
  if [ -d "$d" ]; then
    echo "  skip (exists): skew=$s"
  else
    python gen/make_skewed.py --total-gb "$GB" --row-bytes "$RB" \
      --skew "$s" --partitions "$P" 2>&1 | grep -E '^\[gen\]' || true
  fi
done

echo "### run"
for s in $SKEWS; do
  python runner/run_one.py \
    --input "$SPARK_SKEW_DATA/skew${s}_rb${RB}_p${P}_g${GB}" \
    --skew "$s" --row-bytes "$RB" --cores "$CORES" --exec-mem "$MEM" \
    --partitions "$P" --tag smoke2 --rep 0 2>&1 | grep -E '^\[run\]' || true
done

echo "### parse"
python parse/parse_eventlog.py --tag smoke2
