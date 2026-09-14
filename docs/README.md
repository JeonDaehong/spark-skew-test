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

## 현재 상태 (2026-09-13)

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
- ⬜ S3 (cores-per-executor), S4c (MapStatus/P6), S5·S6 (커널), S7~S9

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
