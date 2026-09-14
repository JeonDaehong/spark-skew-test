#!/usr/bin/env bash
# ============================================================================
# 정리 — 만든 것을 전부 지운다. 과금 0 으로 되돌린다.
#
#   1. 결과를 먼저 회수 (안 하면 영영 잃는다)
#   2. 인스턴스 종료          → 시간당 과금 중단
#   3. 보안 그룹 삭제
#   4. 키페어 삭제 (AWS + 로컬 .pem)
#   5. 남은 리소스 확인 출력  → 정말 0 인지 눈으로 확인
#
# --keep-results 를 주면 pull 을 건너뛴다 (이미 받았을 때).
# ============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STATE="$HERE/.ec2-state"
AWS=${AWS:-aws.exe}
SKIP_PULL=0
[ "${1:-}" = "--keep-results" ] && SKIP_PULL=1

log() { printf '\n\033[1m%s\033[0m\n' "$*"; }

if [ ! -f "$STATE" ]; then
  echo "$STATE 없음 — 이미 정리됐거나 기동한 적 없음."
  REGION=${REGION:-ap-northeast-2}
else
  # shellcheck disable=SC1090
  source "$STATE"
fi

if [ -n "${INSTANCE_ID:-}" ]; then
  if [ "$SKIP_PULL" = "0" ]; then
    log "[1/5] 결과 회수 (마지막 기회)"
    bash "$HERE/ec2-sync.sh" pull || echo "  회수 실패 — 계속 진행할지 확인하세요"
  else
    log "[1/5] 결과 회수 건너뜀 (--keep-results)"
  fi

  log "[2/5] 인스턴스 종료: $INSTANCE_ID"
  $AWS ec2 terminate-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
       --query 'TerminatingInstances[0].CurrentState.Name' --output text 2>&1 | tr -d '\r'
  echo "  종료 대기..."
  $AWS ec2 wait instance-terminated --region "$REGION" --instance-ids "$INSTANCE_ID" 2>/dev/null \
    && echo "  종료 완료 (과금 중단)"
else
  log "[1-2/5] 종료할 인스턴스 없음"
fi

log "[3/5] 보안 그룹 삭제"
if [ -n "${SG_ID:-}" ]; then
  for i in $(seq 1 10); do
    if $AWS ec2 delete-security-group --region "$REGION" --group-id "$SG_ID" 2>/dev/null; then
      echo "  삭제: $SG_ID"; break
    fi
    # ENI 정리에 시간이 걸린다
    printf '.'; sleep 10
  done
  echo
else
  echo "  (없음)"
fi

log "[4/5] 키페어 삭제"
if [ -n "${KEY:-}" ]; then
  $AWS ec2 delete-key-pair --region "$REGION" --key-name "$KEY" 2>/dev/null \
    && echo "  AWS 키페어 삭제: $KEY"
  [ -n "${PEM:-}" ] && [ -f "$PEM" ] && rm -f "$PEM" && echo "  로컬 키 삭제: $PEM"
fi
rm -f "$STATE"

log "[5/5] 남은 리소스 확인"
echo "--- 실행/대기 중인 인스턴스 ---"
$AWS ec2 describe-instances --region "$REGION" \
  --filters "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name]' --output text 2>&1 | tr -d '\r'
echo "--- 볼륨 (EBS는 인스턴스와 함께 삭제되어야 정상) ---"
$AWS ec2 describe-volumes --region "$REGION" \
  --query 'Volumes[].[VolumeId,Size,State]' --output text 2>&1 | tr -d '\r'
echo "--- spark-skew 보안 그룹 ---"
$AWS ec2 describe-security-groups --region "$REGION" \
  --filters "Name=group-name,Values=spark-skew-sg" \
  --query 'SecurityGroups[].GroupId' --output text 2>&1 | tr -d '\r'

cat <<'EOF'

═══════════════════════════════════════════════════════════
  정리 완료.

  위 목록이 전부 비어 있으면 과금되는 것은 없습니다.
  (EBS 볼륨이 남아 있다면 수동 삭제 필요 — 월 $0.08/GB)
═══════════════════════════════════════════════════════════
EOF
