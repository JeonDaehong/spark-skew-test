#!/usr/bin/env bash
# ============================================================================
# 실험용 EC2 스팟 인스턴스 기동 (멱등)
#
# 왜 EC2 인가
#   로컬(WSL2)에서는 block layer / 실제 NVMe / PMU 를 측정할 수 없고, 다른 작업
#   (게임 등)의 CPU·RAM·디스크 경합이 측정을 오염시킨다. docs/01-local-feasibility.md 참조.
#
# 왜 스팟인가
#   on-demand $1.1676/hr vs spot ~$0.21/hr (약 5.6배). 인스턴스를 소모품으로 다룬다:
#   코드는 git, 결과는 ec2-sync.sh 로 회수, 데이터셋은 재생성 가능(결정론적 시드).
#   중단되면 다시 띄우면 된다 (부트스트랩 ~5분).
#
# 만들어지는 것 (전부 ec2-teardown.sh 로 삭제됨)
#   - key pair  spark-skew
#   - security group  spark-skew-sg   (내 IP 에서만 SSH)
#   - spot instance
# ============================================================================
set -euo pipefail

AWS=${AWS:-aws.exe}                       # WSL 에서 Windows AWS CLI 호출
REGION=${REGION:-ap-northeast-2}
AZ=${AZ:-ap-northeast-2d}                 # 스팟 최저가 AZ (조회 결과)
INSTANCE_TYPE=${INSTANCE_TYPE:-m6id.4xlarge}   # 16 vCPU / 64GB / 950GB NVMe
MAX_SPOT=${MAX_SPOT:-0.45}                # 상한. on-demand($1.17)보다 훨씬 낮게
AMI=${AMI:-ami-086a43496cb46286c}         # Ubuntu 24.04 (ap-northeast-2)
ROOT_GB=${ROOT_GB:-60}
KEY=${KEY:-spark-skew}
SG=${SG:-spark-skew-sg}
NAME=${NAME:-spark-skew}

HERE="$(cd "$(dirname "$0")" && pwd)"
STATE="$HERE/.ec2-state"
PEM="$HOME/.ssh/${KEY}.pem"

log() { printf '\n\033[1m%s\033[0m\n' "$*"; }

log "[1/5] 키페어"
if [ -f "$PEM" ] && $AWS ec2 describe-key-pairs --region "$REGION" --key-names "$KEY" >/dev/null 2>&1; then
  echo "  기존 키 재사용: $PEM"
else
  $AWS ec2 delete-key-pair --region "$REGION" --key-name "$KEY" >/dev/null 2>&1 || true
  mkdir -p "$HOME/.ssh"
  $AWS ec2 create-key-pair --region "$REGION" --key-name "$KEY" \
       --query 'KeyMaterial' --output text > "$PEM"
  # Windows CLI 가 CRLF 를 섞을 수 있다
  sed -i 's/\r$//' "$PEM"
  chmod 600 "$PEM"
  echo "  생성: $PEM"
fi

log "[2/5] 보안 그룹 (내 IP 에서만 SSH)"
MYIP=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')
SG_ID=$($AWS ec2 describe-security-groups --region "$REGION" \
        --filters "Name=group-name,Values=$SG" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null | tr -d '\r')
if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
  SG_ID=$($AWS ec2 create-security-group --region "$REGION" --group-name "$SG" \
          --description "spark-skew experiment (SSH from owner IP only)" \
          --query 'GroupId' --output text | tr -d '\r')
  echo "  생성: $SG_ID"
fi
$AWS ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
     --protocol tcp --port 22 --cidr "${MYIP}/32" >/dev/null 2>&1 \
  && echo "  SSH 허용: ${MYIP}/32" || echo "  SSH 규칙 이미 존재 (${MYIP}/32)"

log "[3/5] 스팟 인스턴스 기동"
# user-data 는 Windows CLI 가 읽으므로 Windows 경로로 넘긴다
UD_WIN=$(echo "$HERE/ec2-userdata.sh" | sed 's|^/mnt/\([a-z]\)|\U\1:|')
SUBNET=$($AWS ec2 describe-subnets --region "$REGION" \
         --filters "Name=availability-zone,Values=$AZ" "Name=default-for-az,Values=true" \
         --query 'Subnets[0].SubnetId' --output text | tr -d '\r')

IID=$($AWS ec2 run-instances --region "$REGION" \
  --image-id "$AMI" --instance-type "$INSTANCE_TYPE" --key-name "$KEY" \
  --subnet-id "$SUBNET" --security-group-ids "$SG_ID" \
  --associate-public-ip-address \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$ROOT_GB,VolumeType=gp3,DeleteOnTermination=true}" \
  --instance-market-options "MarketType=spot,SpotOptions={MaxPrice=$MAX_SPOT,SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}" \
  --user-data "fileb://$UD_WIN" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=project,Value=spark-skew}]" \
  --query 'Instances[0].InstanceId' --output text | tr -d '\r')
echo "  instance: $IID"

log "[4/5] running 대기"
$AWS ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP=$($AWS ec2 describe-instances --region "$REGION" --instance-ids "$IID" \
     --query 'Reservations[0].Instances[0].PublicIpAddress' --output text | tr -d '\r')
echo "  public ip: $IP"

cat > "$STATE" <<EOF
REGION=$REGION
INSTANCE_ID=$IID
PUBLIC_IP=$IP
SG_ID=$SG_ID
KEY=$KEY
PEM=$PEM
INSTANCE_TYPE=$INSTANCE_TYPE
LAUNCHED_AT=$(date -u +%FT%TZ)
EOF
echo "  상태 저장: $STATE"

log "[5/5] 부트스트랩 완료 대기 (~5분)"
for i in $(seq 1 60); do
  if ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
         -o ConnectTimeout=5 -i "$PEM" "ubuntu@$IP" \
         'test -f /opt/BOOTSTRAP_DONE' 2>/dev/null; then
    echo "  부트스트랩 완료"
    break
  fi
  printf '.'
  sleep 15
done
echo

cat <<EOF

═══════════════════════════════════════════════════════════
  준비 완료

  SSH    ssh -i $PEM ubuntu@$IP
  동기화 bash env/ec2-sync.sh push
  회수   bash env/ec2-sync.sh pull
  정리   bash env/ec2-teardown.sh      ← 끝나면 반드시

  비용   ~\$0.21/hr (spot, on-demand \$1.1676 대비 5.6배 저렴)
  안전   60분 유휴 시 자동 종료 (해제: touch /opt/NO_AUTO_SHUTDOWN)
═══════════════════════════════════════════════════════════
EOF
