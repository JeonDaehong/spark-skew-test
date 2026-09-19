# Spark Data Skew Deep Dive — 문서 인덱스

> **"Skewed partition 하나의 비용 곡선은 어디서 꺾이는가 —
> 그리고 그 꺾이는 지점은 Spark가 정하는가, JVM/커널이 정하는가?"**

산출물은 논문이 아니라 **기술 블로그 + LinkedIn 포스트 + (가능하면) Spark JIRA**.

| 문서 | 내용 |
|---|---|
| [00-research-triage.md](00-research-triage.md) | 연구 방향 확정. 기본 동작 / baseline / 파면 안 되는 것 / 기존 자료 / gap / RQ 후보 평가 / **최종 RQ와 6개 명제** |
| [01-local-feasibility.md](01-local-feasibility.md) | 로컬(WSL2) 실측. **어느 레이어까지 주장 가능한가** |
| [02-harness.md](02-harness.md) | 하네스 구조, 사용법, 사이징 근거, summary.csv 컬럼 |
| [03-experiment-matrix.md](03-experiment-matrix.md) | full factorial이 불가능한 이유와 **930-run 단계형 설계** |
| [04-novelty-risk.md](04-novelty-risk.md) | **흔한 실험인가?** 정직한 novelty 평가 + 리스크 3개 |
| [10-s1-methodology-and-results.md](10-s1-methodology-and-results.md) | **S1 방법론 + 결과.** 설계 근거·측정 체계·재현성 통제·결과·판정·한계 |
| [10b-s1-ec2-replication.md](10b-s1-ec2-replication.md) | **S1 EC2 재현.** 계단이 머신의 성질인가 Spark의 성질인가 |
| [11-s2-methodology-and-results.md](11-s2-methodology-and-results.md) | **S2 방법론 + 결과.** bytes vs records 비용 분해, 천장 높이, 계단 이동 |
| [12-s4-methodology-and-results.md](12-s4-methodology-and-results.md) | **S4 방법론 + 결과.** AQE의 record-skew 사각지대 (P3) |
| [13-s5-s6-kernel-and-disk.md](13-s5-s6-kernel-and-disk.md) | **S5/S6.** 커널 writeback은 비용이 아니다 (S5는 유효). S6 부분은 docs/15→16 순으로 읽을 것 |
| [16-apparatus-bug-io-cap.md](16-apparatus-bug-io-cap.md) | ⭐ **장치 버그.** io.max가 26% run에서 안 걸렸다 — 걸러내니 1/대역폭 법칙이 ±4%로 나옴 |
| [15-s6v2-contradiction.md](15-s6v2-contradiction.md) | ⛔ **S6 재실험 72 run — "6.1배"가 재현 안 됨.** 양봉 현상, 규칙 2건 철회 |
| [14-spill-anatomy.md](14-spill-anatomy.md) | **spill 해부.** "고정 136.5 MiB"의 정체 — 반올림 착시 + spill하는 task는 200개 중 하나 |
| [17-s3-cores-and-cliff.md](17-s3-cores-and-cliff.md) | ⭐ **S3.** core 수와 cliff — 예측은 맞고 **근거는 틀렸다.** 첫 spill = 0.757×pool/N |
| [18-s6-closing.md](18-s6-closing.md) | ⛔ **S6 종결.** io.max는 이 장비에서 ~50MB/s 이하에서만 계기로 쓸 수 있다 |
| [19-s8-generalization.md](19-s8-generalization.md) | ⭐ **S8.** 결론이 `sort` 밖에서도 서는가 — **법칙은 일반화되고 상수는 안 된다** |
| [21-s8b-per-key-ops.md](21-s8b-per-key-ops.md) | ⭐ **S8b.** window 도 같은 천장(712.0). 집계는 왜 못 재는지 확정 |
| [22-blog-post.md](22-blog-post.md) | ⭐ **기술 블로그 최종 원고.** daehong770.me.kr 스타일(서론/본론/결론+부록) |
| [20-blog-draft.md](20-blog-draft.md) | 블로그 작업 초안 (발견 7개 나열식) — 22 의 재료 |

## 현재 상태 (2026-09-19)

- ✅ Week 0 환경 구축 완료 (WSL2 / pyspark 4.0.1 / JDK 21, 데이터는 ext4=NVMe)
- ✅ 하네스 전 구간 검증 (생성 → 실행 → 파싱 → 플롯)
- ✅ **B1 통과** — 목표 skew와 실측 일치 (1/4/16/64 → 1.44/3.86/15.7/63.3)
- ✅ **S1 완료** (35 run) — **P1 참.** 바이트당 비용이 실행 메모리 풀 포화 지점에서 **+31% 계단**
  - regime A (skew 8–16) 10.2 ms/MB → regime B (skew 32–64) 13.6 ms/MB
  - 계단 크기가 반복 산포(IQR)의 **약 40배** → 노이즈 아님
  - ⚠️ RQ4(커널) 신호 없음 → S5/S6 재설계 필요 · ⚠️ RQ7 이 설정에선 측정 불가
- ✅ **S1 EC2 재현** (35 run) — 계단이 동일 지점에서 재현. peak execution memory가 **MB 단위까지 동일**(712 MiB)
- ✅ **S2 완료** (90 run) — **P2 참. 비용은 바이트가 아니라 레코드로 지불된다**
  - `duration_ms = 1741 ns × records + 4.95 ns × bytes`, **R² = 0.996** (n=90)
  - 레코드 하나가 바이트 하나보다 **352배** 비쌈. 64B 행은 비용의 **88%가 레코드**
  - **좁은 행은 실행 풀을 다 못 쓴다**: 720 MiB 풀에서 64B 행은 **536 MiB**에서 천장 (24% 사용 불가)
  - 포화 지점 이동: skew **12 → 20 → 24** (64 / 256 / 1024 B)
  - ⚠️ RQ4 커널 신호 여전히 없음 (PSI 평탄)
- ✅ **S4 완료** (54+60 run) — **P3 참. AQE는 record-skew를 한 번도 감지하지 못한다**
  - byte-skew 3/3 감지 · record-skew **0/6 감지**
  - AQE 구제: byte-skew skew32에서 19,164ms → 1,127ms (**17배**)
  - 사각지대 비용: record R=64에서 1,274ms → **3,481ms (2.73배)**, AQE는 무반응
  - **AQE가 구제한 파티션(1,618ms)보다 무시한 파티션(3,634ms)이 2.2배 느림**
  - ⚠️ R=64에서 byte skew가 2.51로 상승(순수 record 효과 아님) · 4/60 run OOM
- ✅ **문헌 재확인 완료** (2026-09-14) — **P6 폐기** (SPARK-48290으로 이미 보고됨), P3·P4 표현 하향
- ✅ **S5 완료** (36 run) — **P5 거짓.** `vm.dirty_ratio`를 10배 조여 Dirty를 5,952→587 MiB로 눌렀는데
  task 시간 변화 ±3.6% 이내. `balance_dirty_pages` 동기 블로킹은 발동조차 안 함
- 🔶 **S6 부분 완료** (21/36 run) — **디스크는 넘는 순간에만 비용이다**
  - 쓰기를 50 MB/s로 조이면 skew 64에서 lz4 **6.1배**, none **7.6배** 느려짐 — 그런데 **zstd는 1.00배**
  - 규칙: **`job 전체 쓰기량 / 대역폭`** 이 wall time 하한을 정한다
    (2026-09-16 정정 — 처음엔 `spill/대역폭`으로 썼는데 spill은 총 쓰기의 23%뿐이었다)
  - **최적 codec이 뒤집힌다**: 빠른 디스크 → 압축 끄기(22% 이득), 느린 디스크 → zstd(6배 이득)
  - ⚠️ n=1~2, 36 run 미완 · cap 레벨 2개뿐이라 교차점 미관측
- ✅ **6.1배 증폭 규명** (2026-09-16, 재실행 없이 samples.csv 분석)
  - spill 1,792 MiB vs 실제 디스크 출력 **7,728 MiB** — 나머지는 shuffle write
  - 7,728/50 = 155s ≈ 실측 162s. **설명 안 되던 6.1배는 분모를 잘못 고른 것이었다**
  - zstd가 이기는 이유도 정정: spill이 아니라 **총 쓰기량**이 lz4의 54%라서
- ✅ **136.5 MiB 수수께끼 해결** (2026-09-16, 재실행 없이 tasks.csv 분석)
  - "고정"이 아니었다 — raw는 143,173,819 / 143,167,339 / 143,161,152 bytes로 다른데
    **MiB 반올림이 같아 보이게 만들었다**
  - spill하는 task는 200개 중 **단 하나**(hot 파티션, index 191). "배경"이 아니었다
  - 크기는 파티션 크기가 아니라 **sorter가 첫 할당 실패를 맞은 시점의 누적량**이 정한다
- ⚠️ **S6 종결 — 측정 불가로 닫음** (129 run / 3 스윕, `13`→`15`→`16` 철회 연쇄)
  - v1 "6.1배"는 v2(72 run)에서 재현되지 않았고, 규칙 2건을 철회했다
  - 원인은 장치 버그였다: cgroup `io.max` 가 제한 run 의 26% 에서 **조용히 안 걸렸다**
  - cgroup `io.stat` 으로 검증을 고쳤더니 cap 은 걸렸는데도 분산이 남았다
  - v3(36 run)에서 원인 확정: **장비 자체가 76~239 MiB/s 로 흔들린다.**
    조작량(2배)과 장비 분산(2배)이 같은 크기라 분리가 안 된다 → `18-s6-closing.md`
  - 다만 **충분히 느리면(50MB/s) 5.4배**는 깨끗하게 재현된다
- ✅ **S3 완료** (48 run, 46 성공 / 2 OOM) — **P4 예측은 참, 근거는 거짓**
  - cliff 는 정말 앞당겨진다: cores 2/4/6 에서 skew **32 → 24 → 20** 단조 이동
  - 그런데 "태스크가 풀의 1/N 만 쓴다"는 **어느 core 수에서도 관측되지 않았다.**
    6 core 에서도 hot 태스크 peak 는 **716 MiB** (조언의 예측치 120 MiB, 6배 틀림)
  - 진짜 메커니즘: N 은 peak 가 아니라 **첫 할당 실패 시점**을 정한다.
    첫 spill = 0.757 × pool / N — raw 바이트로 rep 간 5자리까지 일치
  - core 6 에서만 **OOM 2건**. 더 많은 core 의 대가는 "일찍 spill" 이 아니라 힙 사망
  - skew 8→32 이 6 core 확장성을 4.23× → 3.23× 로 **24% 깎는다**
- ✅ **S8 완료** (99 run, 오류 0건) — **법칙은 일반화되고 상수는 안 된다**
  - 계단 메커니즘은 join 에서 재현 (cliff 위치 동일: skew 16→24 사이)
  - 그런데 **천장이 연산자마다 다르다** — sort **712.0** / join **616.1** MiB,
    포화 구간 6 run 씩 소수점까지 동일 (SMJ 는 sorter 가 둘 — 추론)
  - **1/N 법칙도 일반화.** 계수만 다르다: sort 0.757 / join **0.643**
    (join 은 1c·2c·4c 에서 462.8 MiB 로 **퍼짐 0.0%**)
  - 음성 대조군 `count` 통과 — 전 구간 평평(5.4s, peak 8 MiB, spill 0)
  - ⚠️ `agg` 는 **판정 불가** — `peakExecutionMemory` 를 안 보고하고
    partial aggregation 이 shuffle 을 20배 줄여 풀에 닿지도 않는다
  - ⚠️ 레코드 지배는 sort 만 확인(+0.70s vs 산포 0.25s), join 은 산포에 묻혐다.
    1741 ns/record 가 과대예측하는 것까지 **S4 의 한계를 독립 재현**했다
- ✅ **S8b 완료** (32 run) — S8 이 남긴 `agg` 구멍을 닫았다 (`docs/21`)
  - **`window`(row_number)가 sort 와 똑같은 천장 712.0 MiB** — 12 run 전부 단일값.
    천장은 워크로드가 아니라 **어떤 메모리 소비자를 쓰느냐**가 정한다
  - 다만 **spill 양은 일반화 안 된다**: skew 32 에서 window 1,529 vs sort 689 MiB.
    cliff 도 window 16 / sort 24 로 갈린다 (sorter 왕복 — **추론**)
  - ⛔ **집계의 계단은 이 하네스로 측정 불가 — 이유 확정**:
    축약 가능한 집계는 map-side 에서 hot 파티션이 사라지고, 축약 불가능한
    집계(`collect_list`)는 **출력 한 행이 1.15 GB** 라 힙을 넘는다. 그 사이가 없다
  - ⚠️ 실패 6건은 전부 stage 1 에서 났으므로 그 run 들의 `spill=0` 은
    **증거가 아니다** (reduce task 가 없어서 0)
  - 실무: 같은 입력에서 sort/window 는 **1,054 MiB**, `collect_list` 는 **293 MiB** 에서 멈춘다
- ⚠️ **하네스 버그로 S8b 를 한 번 통째로 잃었다** (`bba2377`)
  - 대기 스크립트의 `pgrep -f <sweep>.sh` 가 **자기 명령줄에도 매칭**돼서
    스윕이 끝나도 영원히 RUNNING → pull 안 함 → 60분 유휴 watchdog 이 종료
  - 고침: `env/ec2-sync.sh wait <sweep>` — **완료 표식 파일** 1순위 +
    대괄호 트릭 + **5분마다 중간 회수**. 스윕 8개 전부에 trap 표식 추가
- ⬜ S7·S9 미착수 (S9 PMU 는 권장 안 함) · 최종 산출물: 블로그(`20-blog-draft.md`) / LinkedIn

## 빠른 시작

```bash
bash env/setup-wsl.sh          # 멱등
source ~/.spark-skew-env
bash runner/smoke.sh           # 하네스 검증 (~5분)
bash runner/s1_sweep.sh        # S1 본 실험
python analysis/show.py --tag s1
python analysis/plot_cliff.py --tag s1
```

## 판단이 갈리는 지점

**S1 판정 완료 → 8주 계획 유지.** 조정 3가지는 `10-s1-methodology-and-results.md` §10 참조.

S2가 P2를 확인했고, 그 결과가 **P3를 최우선으로 끌어올렸다**.

AQE의 skew 판정은 `bytesByPartitionId`만 본다 — 바이트만 본다. 그런데 비용의
지배 요인은 레코드다 (좁은 행에서 88%). ⇒ **바이트로는 평범해 보이지만 레코드가
몰린 파티션은 AQE에 안 잡히면서 실제로는 몇 배 비싸다.** S4에서 직접 검증한다.
