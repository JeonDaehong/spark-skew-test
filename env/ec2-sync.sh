#!/usr/bin/env bash
# 코드 올리기 / 결과 회수.
#
#   push   repo(코드만) -> EC2:~/spark-skew
#   pull   EC2 results/ -> 로컬 results/      (데이터셋은 안 가져온다 — 재생성 가능)
#   ssh    셸 접속
#   run    원격에서 명령 실행
#   wait   스윕이 끝날 때까지 기다리며 **주기적으로 회수**한다
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
  wait)
    # 원격 스윕이 끝날 때까지 기다린다.
    #
    # 여기엔 데이터를 날려먹은 버그가 둘 있었다. 둘 다 고쳐 넣는다.
    #
    # (1) `pgrep -f s8b_sweep.sh` 는 **자기 명령줄에도 매칭된다.**
    #     ssh 가 원격에 띄우는 `bash -c '... s8b_sweep.sh ...'` 자신이 걸려서
    #     스윕이 끝나도 영원히 RUNNING 을 반환했다. 대괄호 트릭으로 막는다:
    #     "[s]8b_sweep" 은 프로세스 목록의 리터럴 "s8b_sweep" 에는 맞지만
    #     패턴 문자열 자신에는 안 맞는다.
    #     더 확실하게, 스윕이 끝나며 남기는 **완료 표식 파일**을 1순위로 본다.
    #
    # (2) 회수를 마지막에 한 번만 했다. 그 사이 idle watchdog 이 인스턴스를
    #     종료시키면 전부 잃는다 (실제로 S8b 32 run 을 그렇게 잃었다).
    #     그래서 **5분마다 중간 회수**한다. 최악의 손실을 한 주기로 묶는다.
    shift
    PATTERN="${1:?usage: ec2-sync.sh wait <sweep-script-name> [max-polls]}"
    MAXPOLL="${2:-240}"                      # 30초 x 240 = 2시간
    BRACKET="[${PATTERN:0:1}]${PATTERN:1}"   # s8b_sweep -> [s]8b_sweep
    DONE_MARK="/home/ubuntu/.${PATTERN}.done"

    echo "⏳ wait: $PATTERN (표식 $DONE_MARK, 최대 $((MAXPOLL/2))분, 5분마다 중간 회수)"
    miss=0
    for i in $(seq 1 "$MAXPOLL"); do
      out=$(ssh "${SSH_OPTS[@]}" -o ConnectTimeout=10 -o ServerAliveInterval=30 "$REMOTE" \
            "if [ -f '$DONE_MARK' ]; then echo DONE; elif pgrep -f '$BRACKET' >/dev/null; then echo RUNNING; else echo GONE; fi" \
            2>/dev/null) || out=""
      case "$out" in
        RUNNING) miss=0 ;;
        DONE)    echo "  ✅ 완료 표식 발견 (poll $i)"; break ;;
        GONE)    echo "  ⚠️  표식은 없는데 프로세스도 없다 — 중간에 죽었을 수 있다 (poll $i)"; break ;;
        *)       miss=$((miss+1)); echo "  ssh 실패 $miss 회 (판정 보류)"
                 [ "$miss" -ge 20 ] && { echo "  ssh 10분 불통 — 중단"; break; } ;;
      esac
      # 5분마다 중간 회수. 여기서 잃는 최대치는 5분어치다.
      if [ $((i % 10)) -eq 0 ]; then
        echo "  … 중간 회수 (poll $i)"
        rsync -az -e "ssh ${SSH_OPTS[*]}" "$REMOTE:$RDIR/results/" "$REPO/results/" \
          2>/dev/null || echo "     (중간 회수 실패 — 계속 진행)"
      fi
      sleep 30
    done

    echo "← 최종 회수"
    bash "$HERE/ec2-sync.sh" pull
    ;;
  ssh)
    exec ssh "${SSH_OPTS[@]}" "$REMOTE"
    ;;
  run)
    shift
    ssh "${SSH_OPTS[@]}" "$REMOTE" "source /etc/profile.d/spark-skew.sh && cd $RDIR && $*"
    ;;
  *)
    echo "usage: $0 {push|pull|ssh|run <cmd>|wait <sweep-name> [max-polls]}"
    exit 1
    ;;
esac
