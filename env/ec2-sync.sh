#!/usr/bin/env bash
# 코드 올리기 / 결과 회수.
#
#   push   repo(코드만) -> EC2:~/spark-skew
#   pull   EC2 results/ -> 로컬 results/      (데이터셋은 안 가져온다 — 재생성 가능)
#   ssh    셸 접속
#   run    원격에서 명령 실행
#
# 인스턴스는 소모품이다. 결과는 반드시 pull 해두고 teardown 한다.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(dirname "$HERE")"
STATE="$HERE/.ec2-state"
[ -f "$STATE" ] || { echo "no $STATE — env/ec2-launch.sh 먼저 실행"; exit 1; }
# shellcheck disable=SC1090
source "$STATE"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$PEM")
REMOTE="ubuntu@$PUBLIC_IP"
RDIR="/home/ubuntu/spark-skew"

case "${1:-}" in
  push)
    echo "→ push code to $REMOTE:$RDIR"
    ssh "${SSH_OPTS[@]}" "$REMOTE" "mkdir -p $RDIR"
    rsync -az --delete \
      --include='env/***' --include='gen/***' --include='runner/***' \
      --include='parse/***' --include='analysis/***' --include='docs/***' \
      --exclude='*' \
      -e "ssh ${SSH_OPTS[*]}" "$REPO/" "$REMOTE:$RDIR/"
    ssh "${SSH_OPTS[@]}" "$REMOTE" "chmod +x $RDIR/runner/*.sh $RDIR/env/*.sh 2>/dev/null || true"
    echo "완료"
    ;;
  pull)
    echo "← pull results from $REMOTE"
    mkdir -p "$REPO/results"
    rsync -az -e "ssh ${SSH_OPTS[*]}" "$REMOTE:$RDIR/results/" "$REPO/results/"
    # 환경 리포트도 같이 (재현성 기록)
    scp "${SSH_OPTS[@]}" "$REMOTE:/home/ubuntu/ENV_REPORT.txt" \
        "$REPO/docs/ec2-env-report.txt" 2>/dev/null || true
    echo "완료 → $REPO/results"
    ;;
  ssh)
    exec ssh "${SSH_OPTS[@]}" "$REMOTE"
    ;;
  run)
    shift
    ssh "${SSH_OPTS[@]}" "$REMOTE" "source /etc/profile.d/spark-skew.sh && cd $RDIR && $*"
    ;;
  *)
    echo "usage: $0 {push|pull|ssh|run <cmd>}"
    exit 1
    ;;
esac
