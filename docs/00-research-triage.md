# 00. Research Triage — Apache Spark Data Skew Deep Dive

작성: 2026-09-13 · 상태: 확정 (Week 0)

---

## 프로젝트 정의

Apache Spark의 Data Skew 현상 하나를 잡아, 상위 개념에서 시작해 **필요한 만큼만** CPU / JVM / Linux kernel / storage / hardware 레벨까지 내려가며 직접 실험·측정·검증한다.

**최종 산출물은 논문이 아니다.**

1. 기술 블로그 글
2. LinkedIn 기술 포스트
3. (가능하면) Apache Spark JIRA / Discussion / PR

따라서 "직접 측정해서 확인한 사실"과 "측정을 통해 새롭게 이해한 것"이 핵심이다.

## 기본 철학

식상한 내용을 **버리지 않는다.** 짧게 baseline으로 검증하고 넘어간다.

```
Known behavior
  → 짧은 baseline 검증 (시간/일 단위, 주 단위 아님)
  → 예상과 다른 현상 탐색
  → 원인 분해
  → JVM level → OS level → hardware level
  → causal relationship 검증 (intervention)
```

**깊이의 원칙**: "현재 레이어에서 현상을 설명할 수 있는가?"를 먼저 본다. Spark 메트릭으로 설명되면 더 내려가지 않는다. 설명 안 되면 JVM → Linux → block/hardware 순. **필요한 만큼만 깊게 내려간다.** 도구 중심 접근("CPU cache까지 봤으니 딥다이브")은 하지 않는다.

---

## 1. 반드시 알아야 하는 기본 동작

개념 설명이 아니라 **실험 설계에 직접 영향을 주는 것만**.

### (a) skew는 write side가 아니라 read side에서 아프다

mapper는 각자 비슷한 총 바이트를 쓴다 — reducer bucket 사이에 불균등하게 나눌 뿐. 고통은 reducer 하나가 M개 mapper로부터 자기 파티션 블록을 전부 당겨올 때 발생. → **측정 포인트는 shuffle read 경로.**

### (b) MapStatus는 실제 크기가 아니다 ← 가장 덜 알려진 지점

- `CompressedMapStatus`: 블록당 1바이트, 로그 스케일 압축(≈1.1^x) → 최대 ~10% 오차
- 파티션 수 > `spark.shuffle.minNumPartitionsToHighlyCompressMapStatus` (기본 **2000**) → `HighlyCompressedMapStatus` 전환 → `spark.shuffle.accurateBlockThreshold` (기본 **100MB**) **미만 블록은 개별 크기를 버리고 평균값으로 보고**
- 결과: **AQE가 보는 파티션 크기와 median은 실제값이 아니다.** 판정식 `size > 256MB AND size > 5 × median`의 median이 합성값.
- **파티션 수 2000에서 동작이 이산적으로 바뀐다.** 실험 가능한 진짜 discontinuity.

> TODO: `MapStatus.scala`, `ShufflePartitionsUtil.scala` 소스에서 직접 확인할 것.

### (c) AQE skew 판정은 전적으로 byte 기반이다

`ShufflePartitionsUtil`은 `bytesByPartitionId`만 본다. **record 수는 보지 않는다.** → row가 좁고 record가 많은 skew는 AQE에 안 잡힌다.

### (d) task당 execution memory는 고정이 아니다

Unified memory manager: 실행 메모리 풀을 executor 내 active task N개가 나눠 갖는다. 보장 하한은 1/2N~1/N이지만 **상한은 풀 전체** — 다른 task가 놀면 한 task가 전부 가져갈 수 있다.

→ **executor당 core 수가 skew 내성을 직접 결정한다.** "5 cores per executor" 통념과 충돌.

> 스모크 실측으로 확인: 1g driver(풀 434MB)에서 peak execution memory 310MB까지 spill 없이 버팀. 하한(1/4 = 108MB)이 아니라 풀 전체에 가깝게 쓴다는 증거.

### (e) sorter 비용은 bytes가 아니라 record 수에 걸린다

`UnsafeInMemorySorter`의 `LongArray` = record당 2 long (pointer + prefix) = **16 bytes/record**, 2배씩 grow. 이 배열이 G1에서 `region/2` 넘으면 **humongous allocation**. → record 수가 JVM 레벨 비선형성의 트리거.

> 로컬 실측: 8g heap → `G1HeapRegionSize = 4MB` → humongous 임계 = **2MB** = **131,072 record**. 손쉽게 도달.

### (f) shuffle write path는 3개고 조건에 따라 이산적으로 갈린다

| Writer | 조건 |
|---|---|
| `BypassMergeSortShuffleWriter` | 파티션 ≤ `spark.shuffle.sort.bypassMergeSortThreshold` (200), map-side agg 없음 |
| `UnsafeShuffleWriter` | ≤ 2^24 파티션, agg 없음, serializer relocation 지원 |
| `SortShuffleWriter` | 나머지 |

→ **skew를 키우다가 파티션 수를 건드리면 경로 자체가 바뀌어 비교가 깨진다.** 통제 필수.

### (g) spill write는 page cache로 간다

`spill bytes`가 증가해도 곧바로 디스크 대기가 아니다. 커널이 흡수한다 — `vm.dirty_ratio`(기본 20%) 전까지는. 넘으면 `balance_dirty_pages()`가 **쓰는 스레드를 D-state로 동기 블로킹**한다. → **여기가 물리적으로 실재하는 cliff.**

### (h) 단일 블록 200MB 룰

`spark.maxRemoteBlockSizeFetchToMem` (3.x 기본 **200m**) 초과 블록은 메모리가 아니라 **디스크로 fetch**. 그런데 **Spark UI의 spill 메트릭에 안 잡힌다.** → 메트릭 사각지대. (`parse/parse_eventlog.py`가 `remote_to_disk_total`로 따로 집계한다.)

---

## 2. 짧게 baseline만 찍을 것 (30분 ~ 몇 시간)

목표는 "확인하고 넘어가기". 절대 여기에 주를 쓰지 않는다.

| # | 검증 | 시간 | 예상대로면 | 예상과 다르면 |
|---|---|---|---|---|
| B1 | skew ↑ → partition size 불균등 ↑ | 30분 | 넘어감. 단 **실측 vs MapStatus 보고값 차이**를 기록 | MapStatus 압축 의심 |
| B2 | skewed task duration ↑ | 30분 | 넘어감. **증가율이 선형인지만** 본다 | 즉시 파고듦 |
| B3 | skew ↑ → spill bytes ↑ | 1시간 | 넘어감. **spill bytes와 task time의 상관이 약하면 그게 진짜 시작점** | — |
| B4 | AQE skew handling이 imbalance를 줄인다 | 1시간 | 넘어감 | — |
| B5 | stage 완료시간 ≈ max(task) | 30분 | 넘어감. **동일 executor 내 非skew task까지 느려지는지** 확인 | — |
| B6 | **재현성: 동일 조건 10회 반복** | 2시간 | **필수** | 환경 통제부터 다시 |

> **B6를 건너뛰면 프로젝트 전체가 무효.** cliff을 주장할 건데 noise 수준을 모르면 아무 말도 못 한다.
> WSL2는 cpufreq가 없어 governor 고정은 불필요/불가능. 대신 run 사이 `drop_caches`(구현됨), 최소 5회 반복, median + IQR 보고로 통제한다.

**B1은 이미 통과했다** — `results/smoke2/figures/skew_check.png` 참조. 목표 skew 1/4/16/64 → 실측 1.44/3.86/15.7/63.3.

---

## 3. 깊게 파면 안 되는 것 (카테고리 A)

- skew → task imbalance → stage straggler
- salting / key 재분배로 완화된다
- AQE `skewJoin`이 파티션을 쪼갠다
- broadcast join으로 shuffle 회피
- `groupByKey` vs `reduceByKey`, `repartition` vs `coalesce`
- "spill은 나쁘다", "shuffle은 비싸다"
- Spark UI에서 spill/shuffle 메트릭 읽는 법
- `spark.sql.shuffle.partitions` 튜닝 일반론

→ 블로그 도입부 3문단 분량. **결과가 아니라 배경.**

---

## 4. 기존 자료가 설명하는 범위

**학술 — 거의 유일한 진지한 앵커**

- **Ousterhout et al., _Making Sense of Performance in Data Analytics Frameworks_ (NSDI '15)** — blocked-time analysis. **"디스크 I/O를 완전히 없애도 job time 중앙값 19% 개선에 그친다; 병목은 대개 CPU다."**
  - 이 프로젝트의 방법론적 출발점이자 "spill = disk bound" 통념을 이미 한 번 깬 논문.
  - **한계**: 2015년 Spark(pre-AQE, pre-Tungsten 성숙), 2015년 하드웨어(NVMe 아님), **skew를 통제변수로 다루지 않음**, job 레벨 총계이지 skewed task 내부 분해 아님.
- 최근 학술(Spark 파라미터 자동튜닝 등)은 skew를 **cost model의 feature**로만 쓰고 메커니즘을 보지 않음.

**산업 자료**

- Databricks / AWS Glue / Medium 다수: 전부 **config-level 완화법**. `skewedPartitionThresholdInBytes` 256MB, `skewedPartitionFactor` 5는 잘 문서화됨.
- Facebook _Skew Mitigation for Petabyte-scale Joins_: 프로덕션 완화 전략. 메커니즘 분해 아님.
- Spark JIRA: SPARK-21033, SPARK-32901, SPARK-14363 등 sorter/pointer array OOM 버그는 개별적으로 알려짐. **다만 skew degree의 함수로 측정된 적은 없음.**

**커널**

- `balance_dirty_pages` 스로틀링, PSI, writeback은 커널 문헌에 잘 정리됨. **Spark와 연결한 측정 자료는 사실상 없음.**

---

## 5. Gap — 여기가 이 프로젝트의 자리

1. **곡선의 모양이 없다.** 아무도 `latency = f(skew)`를 그려서 **breakpoint를 찾고 각 구간에 다른 메커니즘을 귀속시키지 않았다.**
2. **bytes와 records가 분리된 적이 없다.** 모든 자료가 "partition size"만 말한다. 그런데 AQE 판정은 byte 기반, sorter/GC 비용은 record 기반이다.
3. **executor 형상(cores-per-executor)과 skew의 상호작용이 측정된 적 없다.**
4. **spill → 커널 → 디스크 구간이 통째로 비어 있다.** **cliff 위치가 Spark config가 아니라 박스의 free RAM에 의해 정해질 가능성**이 검토되지 않았다.
5. **메트릭 사각지대가 문서화되지 않았다.** MapStatus 압축 왜곡, 200MB 초과 블록 fetch-to-disk가 spill로 안 잡히는 것.

---

## 6~9. Research Question 후보 + 평가

점수 5점 만점. **난이도는 높을수록 어려움.** Novelty 분류 A(너무 알려짐)~E(새 threshold 발견 가능).

| # | Research Question | Novelty | 난이도 | Blog | 레이어 |
|---|---|---|---|---|---|
| **RQ1** | skew–latency 곡선의 breakpoint는 몇 개이고 각각 어느 레이어가 만드는가? | C/E · 4 | 3 | **5** | Spark→JVM→OS→HW |
| **RQ2** | cores-per-executor가 skew 내성 임계점을 바꾸는가? | D/E · **5** | **1** | **5** | Spark memory manager |
| **RQ3** | spill 비용은 disk-bound인가 CPU(압축/직렬화)-bound인가? | B/C · 3 | 2 | 4 | Spark→CPU/PMU |
| **RQ4** | cliff 위치를 정하는 건 Spark config가 아니라 free RAM / `vm.dirty_ratio`인가? | D/E · **5** | 5→**3** | **5** | OS page cache |
| **RQ5** | record-skew vs byte-skew: 비용 지배자는? AQE는 record-skew를 놓치는가? | D · **5** | 2 | **5** | Spark→JVM |
| **RQ6** | MapStatus 압축(2000 파티션 / 100MB)이 AQE skew 판정을 왜곡하는가? | D/E · **5** | 2 | 4 | Spark only |
| **RQ7** | 200MB 초과 블록 fetch-to-disk가 "보이지 않는 spill"을 만드는가? | D · 4 | 3 | 4 | Spark→OS I/O |

> RQ4 난이도는 로컬 WSL2에서 PSI/BTF/tracefs가 전부 살아있음을 실측 확인하여 **5 → 3으로 하향**. `01-local-feasibility.md` 참조.

**해석**

- **RQ2, RQ5, RQ6는 novelty가 높은데 난이도가 낮다.** 드문 조합. 초반에 빠르게 확보할 실적.
- **RQ4는 "wow"가 가장 크다.** 단독 주제로 잡으면 6주차에 무결과로 끝날 위험 → 반드시 조합으로.
- **RQ3은 Ousterhout와 겹친다.** 단독 novelty 낮음. **causality 도구**로만 쓰고 "새 발견"으로 포장하지 않는다.
- **RQ6은 Spark JIRA 가능성이 가장 높다.**

---

## 10. 최종 Research Question

> ### "Skewed partition 하나의 비용 곡선은 어디서 꺾이는가 — 그리고 그 꺾이는 지점은 Spark가 정하는가, JVM/커널이 정하는가?"

**설계의 핵심: cliff의 _위치_ 자체를 종속변수로 둔다.**

```
고정:   total input bytes, total cores, total executor memory,
        shuffle partition 수, storage device, Spark version/config

주변수: skew degree (1 → 2 → 4 → 8 → 16 → 32 → 64)
        ⇒ 이걸로 latency cliff를 먼저 찾는다   [S1]

그 다음, cliff 위치가 어떻게 "이동"하는지를 본다:
  교차 A. row width (bytes 고정, record 수 변화)   → Spark sorter / JVM G1   [S2]
  교차 B. cores-per-executor (총합 고정)           → Spark memory manager    [S3]
  교차 C. free RAM & vm.dirty_ratio                → Linux page cache        [S5/S6]
```

### 왜 이 형태인가

1. **correlation 함정을 구조적으로 회피한다.** "spill이 늘고 latency가 늘었다"가 아니라 "A를 바꾸니 cliff이 8x에서 16x로 옮겨갔다"가 나온다. intervention 기반 = 사실상 causal 주장.
2. **레이어 하강이 억지가 아니라 필연이 된다.** 교차변수 3개가 각각 다른 레이어를 건드리므로 "어느 레이어가 threshold를 소유하는가"에 자동으로 답하게 된다.
3. **단계별로 결과가 확보된다.** A(RQ5), B(RQ2)는 순수 Spark 설정만으로 2~3주 내 완료 → 여기서 이미 블로그 하나. C(RQ4)가 실패해도 프로젝트는 실패하지 않는다.
4. **결론이 실무적으로 날이 선다.**

### 검증할 명제 (전부 반증 가능)

| # | 명제 | 반증 조건 |
|---|---|---|
| **P1** | skew 대비 latency는 선형이 아니고 특정 지점에서 기울기가 급변한다 | 기울기 일정 또는 변화가 **반복 IQR 안**에 들어감 |
| **P2** | 그 지점 위치는 bytes보다 **record 수**로 더 잘 예측된다 | row 폭을 바꿔도 cliff이 **같은 byte 지점**에 머무름 |
| **P3** | **AQE는 record-skew를 감지하지 못한다** | byte-균등·record-불균등 데이터에서 AQE skew split 발동 |
| **P4** | 총 메모리 고정 시 executor당 core를 늘리면 cliff이 **낮은 skew로 당겨온다** | cliff 위치가 core 수와 무관 |
| **P5** | Spark 설정 불변인 채 **free RAM만 바꿔도 cliff이 이동한다** | free RAM 4배 변화에도 cliff 고정 |
| **P6** | 파티션 2000+ 에서 **AQE가 보는 크기가 실측과 유의하게 다르다** | 차이가 무시할 수준 |

### 목표 결론 형태 (가정 아님, 도달하고 싶은 서술의 형태)

> "Spark 메트릭만 보면 단일 skew 문제로 보이지만, cliff의 위치는 Spark config가 아니라 _(record 수 / task당 메모리 지분 / page cache 여유)_ 가 결정했다. 즉 skew의 임계점은 Spark 레이어에 있지 않다."

---

## Novelty 정직성 — 반드시 지킬 것

`04-novelty-risk.md`에 상세. 요약:

- **"블로그에 없다 ≠ 새롭다."** P3, P6는 소스를 읽으면 보이는 것들이다.
- 정직한 표현은 *"새로 발견했다"*가 아니라 ***"알려질 수 있었지만 아무도 측정해서 보여주지 않은 것을 측정했다"***.
- 2주차 시작 전 **Spark JIRA 전수 검색 + dev@spark 메일링 리스트 + GitHub 이슈**로 P3/P5/P6를 재확인한다. 이미 있으면 그 항목은 버린다.

---

## 8주 개요

| 주차 | 내용 | 산출 |
|---|---|---|
| 0 | WSL2 환경, 하네스, 스모크 | `env/` `gen/` `runner/` `parse/` `analysis/` ✅ |
| 1 | Baseline B1–B6 + **S1 cliff 탐색** | cliff 존재 여부 확정 |
| 2 | 교차 A (row width) — RQ5 + RQ6 | **블로그 #1 초안** |
| 3 | 교차 B (cores-per-executor) — RQ2 | LinkedIn #1 |
| 4 | JVM 하강: async-profiler / JFR / GC log, G1 humongous | 증거 |
| 5–6 | OS 하강: PSI, `/proc/vmstat`, `balance_dirty_pages` tracepoint — RQ4 | **핵심 결과 or 기각** |
| 7 | Intervention (RQ3 포함) + EC2에서 PMU / 실제 NVMe | causality |
| 8 | 블로그 + LinkedIn + (RQ6 서면) JIRA | 산출물 |

## Kill criteria

- **2주차 끝에 cliff이 재현 가능한 형태로 안 보이면** → RQ4를 버리고 RQ5 + RQ6 + RQ2로 축소. 이것만으로도 블로그 하나는 나온다.
- **5주차 끝에 커널 레벨 증거가 안 나오면** → "Spark/JVM 레이어에서 설명이 끝났다"를 결론으로 삼는다. 이것도 정당한 결과다 (깊이의 원칙).

---

## 절대 하지 말 것

- 이미 잘 알려진 내용을 장황하게 설명
- 모든 레이어를 억지로 조사
- metric이 많다는 이유로 좋은 연구라고 생각
- correlation을 causation으로 표현
- benchmark 숫자 하나로 일반화
- 특정 환경 결과를 Spark 전체의 법칙처럼 표현
- novelty 과장
- 도구 중심 접근
- 블로그를 예쁘게 만드는 것을 연구보다 우선

## 각 실험 기록 템플릿

```
### Question           무엇을 알고 싶은가?
### Existing knowledge 이미 알려진 사실인가?
### Why investigate?   왜 직접 검증할 가치가 있는가?
### Hypothesis         무엇을 예상하는가?
### Experiment         정확히 무엇을 고정하고 무엇을 변경하는가?
### Metrics            무엇을 측정하는가?
### Expected result    무엇을 예상하는가?
### Falsification      어떤 결과면 가설이 틀렸다고 볼 것인가?
### Next branch        결과 A면 어디로, 결과 B면 어디로?
### Blog value         이 결과가 블로그에서 어떤 insight가 되는가?
```

## 최종 블로그 구조

```
Problem → Common explanation → Short baseline experiment
→ Unexpected observation → Investigation
→ JVM-level evidence → OS-level evidence → Hardware-level evidence
→ Causal validation → Mitigation → Trade-off → Practical conclusion
```

LinkedIn은 이 중 `Unexpected observation → Why → Evidence → Conclusion`만 압축.

---

## 참고 자료

- [Making Sense of Performance in Data Analytics Frameworks (NSDI '15, PDF)](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-ousterhout.pdf) · [the morning paper 요약](https://blog.acolyer.org/2015/04/20/making-sense-of-performance-in-data-analytics-frameworks/)
- [AQE 파라미터 정리 (Datumo)](https://www.datumo.io/blog/useful-aqe-parameters-you-may-find-in-spark-sql-conf) · [Databricks AQE 문서](https://docs.databricks.com/aws/en/optimizations/aqe)
- [SPARK-21033](https://issues.apache.org/jira/browse/SPARK-21033) · [SPARK-32901](https://issues.apache.org/jira/browse/SPARK-32901)
- [UnsafeExternalSorter & SortExec deep dive](https://dataninjago.com/2022/01/23/spark-sql-query-engine-deep-dive-15-unsafeexternalsorter-sortexec/)
- [Linux page cache writeback 내부](https://kernel-internals.org/io/page-cache-writeback/) · [Linux buffered write latency](https://dev.to/fritshooglandyugabyte/linux-buffered-write-latency-10mc) · [mm/page-writeback.c](https://github.com/torvalds/linux/blob/master/mm/page-writeback.c)
- [Optimize shuffles — AWS Prescriptive Guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html) · [Skew Mitigation for Facebook's Petabyte-scale Joins](https://www.slideshare.net/slideshow/skew-mitigation-for-facebook-petabytescale-joins/239587209)
