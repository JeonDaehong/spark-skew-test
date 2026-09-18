#!/usr/bin/env bash
# ============================================================================
# 정리 — 만든 것을 전부 지운다. 과금 0 으로 되돌린다.
#
#   1. 결과를 먼저 회수 (안 하면 영영 잃는다)
#   2. 인스턴스 종료          → 시간당 과금 중단
#   3. 보안 그룹 삭제
#   4. 키페어 삭제 (AWS + 로컬 .pem)
#   5. spot 요청 취소 확인    → persistent 였다면 새 인스턴스가 다시 뜬다
#   6. 남은 리소스 확인 출력  → 인스턴스/볼륨/ENI/EIP/SG/키 정말 0 인지
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
    log "[1/6] 결과 회수 (마지막 기회)"
    bash "$HERE/ec2-sync.sh" pull || echo "  회수 실패 — 계속 진행할지 확인하세요"
  else
    log "[1/6] 결과 회수 건너뜀 (--keep-results)"
  fi

  log "[2/6] 인스턴스 종료: $INSTANCE_ID"
  $AWS ec2 terminate-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
       --query 'TerminatingInstances[0].CurrentState.Name' --output text 2>&1 | tr -d '\r'
  echo "  종료 대기..."
  $AWS ec2 wait instance-terminated --region "$REGION" --instance-ids "$INSTANCE_ID" 2>/dev/null \
    && echo "  종료 완료 (과금 중단)"
else
  log "[1-2/6] 종료할 인스턴스 없음"
fi

log "[3/6] 보안 그룹 삭제"
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

log "[4/6] 키페어 삭제"
if [ -n "${KEY:-}" ]; then
  $AWS ec2 delete-key-pair --region "$REGION" --key-name "$KEY" 2>/dev/null \
    && echo "  AWS 키페어 삭제: $KEY"
  [ -n "${PEM:-}" ] && [ -f "$PEM" ] && rm -f "$PEM" && echo "  로컬 키 삭제: $PEM"
fi
rm -f "$STATE"

log "[5/6] spot 요청 확인"
# SpotInstanceType=one-time 이라 인스턴스 종료로 요청도 닫힌다. persistent 였다면
# 종료하는 순간 새 인스턴스가 다시 떠서 과금이 계속된다 — 그래서 눈으로 확인한다.
OPEN_SPOT=$($AWS ec2 describe-spot-instance-requests --region "$REGION" \
  --filters "Name=state,Values=open,active" \
  --query 'SpotInstanceRequests[].SpotInstanceRequestId' --output text 2>/dev/null | tr -d '\r')
if [ -n "$OPEN_SPOT" ]; then
  echo "  열린 요청 발견: $OPEN_SPOT — 취소합니다"
  # shellcheck disable=SC2086
  $AWS ec2 cancel-spot-instance-requests --region "$REGION" \
    --spot-instance-request-ids $OPEN_SPOT \
    --query 'CancelledSpotInstanceRequests[].[SpotInstanceRequestId,State]' --output text 2>&1 | tr -d '\r'
else
  echo "  열린 spot 요청 없음"
fi

log "[6/6] 남은 리소스 확인"
echo "--- 인스턴스 (전체 — 다른 프로젝트 것이 섞여 있을 수 있다) ---"
$AWS ec2 describe-instances --region "$REGION" \
  --filters "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name,Tags[?Key==`Name`]|[0].Value]' \
  --output text 2>&1 | tr -d '\r'
echo "--- 볼륨 (available = 주인 없는 고아, 삭제 대상) ---"
$AWS ec2 describe-volumes --region "$REGION" \
  --query 'Volumes[].[VolumeId,Size,State,Attachments[0].InstanceId]' --output text 2>&1 | tr -d '\r'
echo "--- 사용 안 되는 ENI ---"
$AWS ec2 describe-network-interfaces --region "$REGION" \
  --filters "Name=status,Values=available" \
  --query 'NetworkInterfaces[].[NetworkInterfaceId,Description]' --output text 2>&1 | tr -d '\r'
echo "--- 연결 안 된 Elastic IP (미연결이면 시간당 과금된다) ---"
$AWS ec2 describe-addresses --region "$REGION" \
  --query 'Addresses[?AssociationId==`null`].[PublicIp,AllocationId]' --output text 2>&1 | tr -d '\r'
echo "--- spark-skew 보안 그룹 ---"
$AWS ec2 describe-security-groups --region "$REGION" \
  --filters "Name=group-name,Values=spark-skew-sg" \
  --query 'SecurityGroups[].GroupId' --output text 2>&1 | tr -d '\r'
echo "--- spark-skew 키페어 ---"
$AWS ec2 describe-key-pairs --region "$REGION" \
  --filters "Name=key-name,Values=spark-skew" \
  --query 'KeyPairs[].KeyName' --output text 2>&1 | tr -d '\r'

cat <<'EOF'

═══════════════════════════════════════════════════════════
  정리 완료.

  위 목록이 전부 비어 있으면 과금되는 것은 없습니다.
  (EBS 볼륨이 남아 있다면 수동 삭제 필요 — 월 $0.08/GB)
═══════════════════════════════════════════════════════════
EOF
