# 01. 로컬 환경 실측 — 어느 레이어까지 내려갈 수 있는가

측정: 2026-09-13 · 대상: 개인 데스크톱 (Windows 11 + WSL2)

이 문서의 목적은 **"어디까지 주장할 수 있고 어디부터는 주장하면 안 되는가"**를 실측으로 못박는 것이다. 이 경계를 블로그에도 그대로 명시한다.

---

## 하드웨어 / 환경

| 항목 | 값 |
|---|---|
| CPU | AMD Ryzen 5 5600X — 6C / 12T, L2 3MB, L3 32MB, 3.7GHz |
| RAM | 32 GB (WSL2에 16 GB 할당, `.wslconfig` 없음 = 기본 50%) |
| NVMe | SK hynix SHGP31-500GM (C:, 466GB / 212GB free) |
| HDD | WDC WD20EZBX 2TB SATA (D:, 1047GB free) |
| WSL2 커널 | **6.18.33.2-microsoft-standard-WSL2**, Ubuntu 24.04 |
| cgroup | **v2** (`cgroup2fs`) |
| JDK | Temurin 21.0.10 (Windows) / OpenJDK 21.0.12 (WSL) |
| Spark | pyspark **4.0.1** (venv, ext4) |
| swap | 4 GB |

**`ext4.vhdx` 위치**: `C:\Users\user\AppData\Local\wsl\{...}\ext4.vhdx` (63.3GB 할당)
→ **NVMe 위에 있다.** HDD가 아님. 데이터/shuffle 경로로 쓸 수 있다.

> **`/mnt/d`는 절대 shuffle/spill 경로로 쓰지 말 것.** SATA HDD + 9p 파일시스템이라 측정이 전부 오염된다. 코드(repo)만 거기 두고, 데이터·venv·`spark.local.dir`은 전부 ext4(`/root/...`)에 둔다.

---

## 레이어별 판정

| 레이어 | 항목 | 판정 | 실측 근거 |
|---|---|---|---|
| **Spark** | 전 항목 | ✅ 완전 가능 | — |
| **JVM** | GC / allocation / JFR / async-profiler | ✅ 완전 가능 | JDK 21 |
| **JVM** | G1 humongous 임계 | ✅ **오히려 유리** | 8g heap → `G1HeapRegionSize=4MB` → humongous = **2MB** = LongArray 기준 **131,072 record**. 쉽게 도달 |
| **Linux** | PSI (cpu/io/memory) | ✅ 가능 | `/proc/pressure/*` 존재, 실제 값 나옴 (`io some avg10=2.62`) |
| **Linux** | page cache / dirty / writeback | ✅ 가능 | `/proc/meminfo` Dirty·Writeback 노출, `vm.dirty_ratio=20` 읽기·쓰기 가능 |
| **Linux** | cgroup v2 메모리 제한 | ✅ 가능 | `cgroup2fs` |
| **Linux** | tracepoint / kprobe / uprobe | ✅ 가능 | tracefs 마운트됨 (`/sys/kernel/tracing`) |
| **Linux** | bpftrace | ✅ 가능 (설치 필요) | `/sys/kernel/btf/vmlinux` 존재 (6.6MB) |
| **Linux** | perf — software event / tracepoint | ⚠️ **빌드 필요** | `/usr/bin/perf`는 래퍼 스텁. MS 커널이라 `linux-tools-6.18.33.2-microsoft`가 apt에 없음 → [WSL2-Linux-Kernel](https://github.com/microsoft/WSL2-Linux-Kernel) 소스에서 직접 빌드 |
| **Block/Storage** | 디바이스 레이턴시 · queue depth · NVMe | ❌ **불가능** | **`dd oflag=direct`가 6.0 GB/s.** 이 SSD 물리 한계는 ~3GB/s → direct I/O가 디바이스에 도달하지 않고 호스트 Hyper-V/NTFS가 흡수 중. `lsblk`가 `rotational=1`로 오보고 |
| **Hardware** | cycles / IPC / cache-miss / branch-miss | ❌ **거의 확실히 불가능** | Hyper-V가 게스트에 vPMU 미노출. perf 빌드 후 즉시 확인 필요하나 `<not supported>` 예상 |
| **Hardware** | NUMA / memory bandwidth | ❌ 불가능 | 단일 소켓 + VM |

**추가 사실**

- `sudo`가 **무비밀번호** (WSL 기본 사용자가 root) → `drop_caches`, `sysctl`, cgroup 조작을 스크립트에서 자유롭게 할 수 있다.
- **cpufreq 없음** (`/sys/devices/system/cpu/cpu0/cpufreq` 부재) → CPU governor 고정은 불가능하고 **불필요**. 대신 재현성은 `drop_caches` + 반복 측정 + IQR 보고로 확보한다.
- I/O scheduler는 전 디바이스 `[none]`.

---

## 결론: 어디까지 주장할 수 있는가

```
✅ 로컬에서 주장 가능
   Spark layer      — 전부
   JVM layer        — 전부 (G1 humongous 포함)
   Linux layer      — page cache, dirty/writeback, PSI, scheduler,
                       context switch, cgroup 메모리 압력

❌ 로컬에서 주장 불가 (EC2 필요)
   Block layer      — 실제 디바이스 레이턴시, NVMe queue depth
   Hardware         — cycles, IPC, cache-miss, memory bandwidth
```

**커버되는 RQ**: RQ1, RQ2, RQ4, RQ5, RQ6, RQ7 — 그리고 RQ3의 CPU 쪽 절반.
**RQ4(page cache / writeback)가 로컬에서 된다는 게 가장 큰 소득**이다. 난이도 5 → 3으로 하향된 근거가 이것이다.

### Week 7 EC2 계획

하드웨어 레이어만 1~2일. `i4i.large` 또는 `m6id.large` 스팟 (local NVMe 필수), Ubuntu, **EMR/Databricks/관리형 k8s 금지** (perf·sysctl·tracepoint 불가). cliff 양옆 2~3점만 재측정. 예상 비용 $5~10.

---

## 남은 리스크

1. **P5(free RAM이 cliff을 정한다)가 물리적으로 안 터질 수 있다.**
   16GB 박스에서 `dirty_background_ratio` 10% = 1.6GB. 스테이지당 spill 총량이 이보다 작으면 writeback 스로틀링이 발동조차 안 한다. 게다가 WSL2는 호스트가 쓰기를 흡수한다.
   → 완화: executor 메모리를 작게 잡아 spill 총량을 키우고, `vm.dirty_ratio`를 5%까지 낮춰 **강제로 발동시킨 뒤** 정상값에서도 재현되는지 확인.

2. **cliff이 계단이 아니라 완만한 무릎일 수 있다.** Spark는 점진적으로 spill한다. 무릎도 비선형이라 쓸 수는 있으나 "phase transition" 표현은 못 쓴다. → S1에서 바로 판명.

3. **디스크 여유.** ext4.vhdx가 C:(212GB free)에 있으므로 **데이터셋 총량 100GB 이하**를 유지한다. S1은 8GB × 7 = 56GB.

---

## 재현용 명령

```bash
# 환경 구축 (멱등)
bash env/setup-wsl.sh
source ~/.spark-skew-env

# 하네스 스모크 (1GiB, ~5분)
bash runner/smoke.sh

# S1 본 실험 (8GiB × 7 skew × 5 rep)
bash runner/s1_sweep.sh
```
