#!/bin/bash
# EC2 부트스트랩 (user-data 로 실행됨, root 권한)
# 로그: /var/log/cloud-init-output.log  ·  완료 표시: /opt/BOOTSTRAP_DONE
set -x
exec > >(tee -a /var/log/spark-skew-bootstrap.log) 2>&1

echo "=== [1/6] instance store NVMe 마운트 ==="
# EBS 루트가 아닌 NVMe(= 인스턴스 스토어)를 찾아 /data 에 붙인다.
# 데이터셋·shuffle·spill 은 전부 여기. 실제 NVMe 라서 로컬 WSL2 의 한계가 사라진다.
ROOT_DEV=$(findmnt -no SOURCE / | sed 's/p\?[0-9]*$//')
STORE=""
for d in /dev/nvme*n1; do
  [ "$d" = "$ROOT_DEV" ] && continue
  lsblk -no MOUNTPOINT "$d" | grep -q . && continue
  STORE="$d"; break
done
if [ -n "$STORE" ]; then
  mkfs.ext4 -F -E nodiscard "$STORE"
  mkdir -p /data
  mount -o noatime "$STORE" /data
  echo "$STORE /data ext4 defaults,nofail,noatime 0 2" >> /etc/fstab
  chmod 777 /data
  echo "instance store mounted: $STORE -> /data"
else
  echo "WARNING: instance store 를 못 찾음. EBS 루트를 쓴다 (측정 신뢰도 낮음)"
  mkdir -p /data && chmod 777 /data
fi

echo "=== [2/6] apt packages ==="
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
  openjdk-21-jdk-headless python3-venv python3-pip \
  sysstat bpftrace git rsync unzip jq \
  linux-tools-common linux-tools-generic "linux-tools-$(uname -r)" || true

echo "=== [3/6] perf / PMU 확인 (로컬에서 불가능했던 것) ==="
sysctl -w kernel.perf_event_paranoid=-1
echo "kernel.perf_event_paranoid=-1" >> /etc/sysctl.d/99-spark-skew.conf
{
  echo "--- perf version ---"; perf --version
  echo "--- HW counters? ---"
  perf stat -e cycles,instructions,cache-misses,branch-misses -- sleep 0.2
} > /opt/PMU_CHECK.txt 2>&1
cat /opt/PMU_CHECK.txt

echo "=== [4/6] python venv + pyspark ==="
python3 -m venv /opt/venv
/opt/venv/bin/pip install -q --upgrade pip
/opt/venv/bin/pip install -q "pyspark==4.0.1" pandas pyarrow matplotlib

mkdir -p /data/spark-skew-data /data/spark-skew-scratch/local \
         /data/spark-skew-scratch/eventlog /data/spark-skew-scratch/warehouse
chmod -R 777 /data

cat > /etc/profile.d/spark-skew.sh <<'EOF'
export SPARK_SKEW_VENV=/opt/venv
export SPARK_SKEW_DATA=/data/spark-skew-data
export SPARK_SKEW_SCRATCH=/data/spark-skew-scratch
export SPARK_SKEW_REPO=/home/ubuntu/spark-skew
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=/opt/venv/bin:$PATH
EOF
# 로컬 하네스가 ~/.spark-skew-env 를 source 하므로 동일 이름으로도 깔아둔다
cp /etc/profile.d/spark-skew.sh /home/ubuntu/.spark-skew-env
chown ubuntu:ubuntu /home/ubuntu/.spark-skew-env

echo "=== [5/6] 유휴 자동 종료 워치독 (비용 안전장치) ==="
# 실험이 끝났는데 인스턴스를 켜둔 채 잊는 것을 막는다.
# 15분 load average 가 임계 미만인 상태가 연속 6회(=60분)면 셧다운.
# 스팟이므로 셧다운 = 종료. /opt/NO_AUTO_SHUTDOWN 을 만들면 비활성화.
cat > /usr/local/bin/idle-watchdog.sh <<'EOF'
#!/bin/bash
THRESHOLD=1.0
NEED=6
STATE=/var/run/idle-count
[ -f /opt/NO_AUTO_SHUTDOWN ] && { echo 0 > $STATE; exit 0; }
LOAD=$(awk '{print $3}' /proc/loadavg)
if [ "$(echo "$LOAD < $THRESHOLD" | bc -l)" = "1" ]; then
  N=$(( $(cat $STATE 2>/dev/null || echo 0) + 1 ))
  echo $N > $STATE
  logger -t idle-watchdog "idle $N/$NEED (load15=$LOAD)"
  [ "$N" -ge "$NEED" ] && { logger -t idle-watchdog "SHUTDOWN"; shutdown -h now; }
else
  echo 0 > $STATE
fi
EOF
chmod +x /usr/local/bin/idle-watchdog.sh
apt-get install -y -qq bc
cat > /etc/systemd/system/idle-watchdog.timer <<'EOF'
[Unit]
Description=idle shutdown watchdog
[Timer]
OnBootSec=30min
OnUnitActiveSec=10min
[Install]
WantedBy=timers.target
EOF
cat > /etc/systemd/system/idle-watchdog.service <<'EOF'
[Unit]
Description=idle shutdown watchdog
[Service]
Type=oneshot
ExecStart=/usr/local/bin/idle-watchdog.sh
EOF
systemctl daemon-reload
systemctl enable --now idle-watchdog.timer

echo "=== [6/6] 환경 리포트 ==="
{
  echo "## CPU";        lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Core|MHz'
  echo "## MEM";        free -g
  echo "## DISK";       lsblk -o NAME,SIZE,ROTA,TYPE,MOUNTPOINT
  echo "## PSI";        cat /proc/pressure/io
  echo "## BTF";        ls -la /sys/kernel/btf/vmlinux
  echo "## CGROUP";     stat -fc %T /sys/fs/cgroup
  echo "## DIRTY";      sysctl vm.dirty_ratio vm.dirty_background_ratio
  echo "## GOVERNOR";   cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "no cpufreq"
  echo "## JAVA";       java -version 2>&1 | head -1
  echo "## PYSPARK";    /opt/venv/bin/python -c 'import pyspark;print(pyspark.__version__)'
  echo "## PMU";        cat /opt/PMU_CHECK.txt
} > /opt/ENV_REPORT.txt 2>&1
cp /opt/ENV_REPORT.txt /home/ubuntu/ENV_REPORT.txt
chown ubuntu:ubuntu /home/ubuntu/ENV_REPORT.txt

touch /opt/BOOTSTRAP_DONE
echo "=== BOOTSTRAP DONE ==="
