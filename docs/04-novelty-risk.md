# 04. Novelty 정직성 평가 — 우리가 하려는 게 흔한 실험인가?

이 문서의 목적은 **나중에 블로그를 쓸 때 과장하지 않기 위한 사전 기록**이다. 결과가 나온 뒤에 novelty를 판단하면 반드시 부풀려진다.

---

## 부분적으로 흔하다. 나눠서 본다.

### 이미 넘치게 있는 것 — 전체의 약 30%, 의도적으로 1주만 씀

| 내용 | 어디에 있나 |
|---|---|
| skew → task 시간 불균등 | Medium, Databricks, AWS 문서, 수십 개 |
| spill 발생 → 느려짐 | 동일 |
| AQE 켜면 나아짐 before/after | 동일 |
| salting으로 완화 | 동일 |
| task duration 분포 히스토그램 | Medium에 흔함 |

→ **블로그 도입부.** 여기에 시간 쓰면 실패다.

### 검색으로 못 찾은 것 — 확신도와 리스크를 같이 기록

| 명제 | 못 찾았다는 확신 | "사실은 이미 알려진 것"일 위험 |
|---|---|---|
| **P5** free RAM이 cliff 위치를 정한다 | **높음** — Spark와 `balance_dirty_pages`를 연결한 측정 자료 자체가 없음 | **낮음.** 다만 *효과가 아예 안 나올* 위험은 별개로 있음 |
| **P4** cores-per-executor × skew 상호작용 | **높음** | **낮음.** 1/N 메모리 분배는 문서화돼 있지만 skew 내성과 연결해 측정한 자료는 못 찾음 |
| **P1** cliff / breakpoint 곡선 자체 | **높음** | **낮음.** `latency=f(skew)` 곡선을 그려 breakpoint를 레이어에 귀속시킨 자료 없음 |
| **P2** record vs byte 분리 (row 폭 통제변수) | **높음** | **중간.** 통제변수로 쓴 실험은 못 찾았으나 "레코드 많으면 느리다"는 직관은 흔함 |
| **P3** AQE가 record-skew를 놓친다 | 중간 | **중상.** AQE가 byte 기반인 건 코드 읽으면 바로 보임. 커미터에겐 자명할 수 있음 |
| **P6** MapStatus 압축이 AQE median을 왜곡 | 중간 | **중상.** `accurateBlockThreshold` 설정 존재 자체가 "알려진 문제"라는 뜻. 다만 **AQE 판정에 미치는 영향을 측정한 자료**는 없음 |

---

## 반드시 지킬 3가지

### 1. 검색 근거의 한계를 인정한다

2026-09-13 시점 **웹 검색 6개 쿼리**가 전부다. 체계적 문헌조사가 아니다.

> **2주차 시작 전 필수 작업**: P3 / P5 / P6에 대해
> - Apache Spark JIRA 전수 검색
> - `dev@spark.apache.org` 메일링 리스트 아카이브
> - GitHub `apache/spark` 이슈·PR
>
> 여기서 이미 있다고 나오면 **그 항목은 버린다.**

### 2. RQ3은 Ousterhout와 겹친다

"spill이 disk-bound인가 CPU-bound인가"는 NSDI '15가 이미 다뤘다 (job 레벨, skew 미통제, 2015 하드웨어라는 차이는 있음).
→ **단독으로 새롭지 않다. causality 검증 도구로만 쓰고 "새 발견"으로 포장하지 않는다.**

### 3. "블로그에 없다 ≠ 새롭다"

P3, P6는 소스를 읽으면 보이는 것들이다. 정직한 표현은

- ❌ "새로운 사실을 발견했다"
- ✅ **"알려질 수 있었지만 아무도 측정해서 보여주지 않은 것을 측정했다"**

이 톤을 지키면 과장 논란이 없고, 오히려 신뢰를 얻는다.

---

## 가장 큰 리스크 3개

### 리스크 1 — cliff이 없을 수도 있다

Spark는 메모리가 부족해지면 **점진적으로** spill한다. 계단이 아니라 완만한 무릎일 가능성이 실재한다. 무릎도 비선형이니 쓸 수는 있지만 "phase transition" 표현은 못 쓴다.

→ **S1(35 run, ~1.5시간)에서 바로 판명난다.**

### 리스크 2 — P5가 물리적으로 안 터질 수 있다

16GB 박스에서 `dirty_background_ratio` 10% = 1.6GB. 스테이지당 spill 총량이 이보다 작으면 writeback 스로틀링이 발동조차 안 한다. 게다가 WSL2는 호스트가 쓰기를 흡수한다 (`dd oflag=direct`가 6 GB/s로 나온 게 증거).

→ 완화: executor 메모리를 작게 잡아 spill 총량을 키우고, `vm.dirty_ratio`를 5%까지 낮춰 **강제로 발동시킨 뒤** 정상값에서도 재현되는지 확인.

### 리스크 3 — WSL2 환경이 결과를 오염시킨다

블록 레이어·디바이스 레이턴시 수치는 쓸 수 없다 (실측 확인됨).

→ Spark / JVM / 커널 page cache 레이어까지만 주장한다. 디스크 관련 주장은 Week 7 EC2에서만. **블로그에 환경 한계를 명시한다.**

---

## 한 문장 요약

> **"Spark skew가 느리다"를 검증하는 게 아니라, "느려지기 시작하는 임계점의 위치를 무엇이 결정하는가"를 개입 실험으로 검증한다.**
> baseline(흔한 부분)은 1주에 소진하고, 나머지 7주는 임계점이 row 폭 / executor 코어 수 / free RAM에 따라 **움직이는지**를 본다.

---

# 문헌 재확인 결과 (2026-09-14)

어제 "블로그 쓰기 전 필수"로 남겨둔 작업. Spark JIRA · dev@ 메일링 아카이브 · GitHub 소스를
직접 확인했다. **한 항목이 이미 보고돼 있어 폐기한다.**

## 판정표

| 명제 | 확인한 것 | 판정 |
|---|---|---|
| **P1** cliff 존재 | `latency=f(skew)` 곡선을 그려 breakpoint 를 레이어에 귀속시킨 자료를 못 찾음 | **유지** |
| **P2** record vs byte | 총 바이트 고정 + row 폭 통제로 비용을 분해한 실험을 못 찾음 | **유지** |
| **P3** AQE record-skew 맹점 | **소스로 확정.** 다만 한계 자체는 일부 블로그에 언급돼 있음 | **유지 (표현 하향)** |
| **P4** cores × skew | **메커니즘도 예측도 이미 문서화돼 있음.** 측정만 없음 | **유지 (표현 대폭 하향)** |
| **P5** free RAM 이 cliff 을 정함 | Spark spill 과 OS page cache/writeback 을 연결한 측정 자료 없음 | **유지** |
| **P6** MapStatus 압축이 AQE 왜곡 | **SPARK-48290 으로 이미 보고됨** | ❌ **폐기** |

---

## P6 — 폐기

**[SPARK-48290] AQE not working when joining dataframes with more than 2000 partitions**
(2024-05-15 등록, Spark 3.5.1/3.3.2, 미해결)

> 파티션 통계가 min/median/max 전부 780925482 로 동일하게 나와 skew 가 감지되지 않는다.
> `spark.shuffle.accurateBlockThreshold` 를 1MB 까지 낮춰도 효과가 없었다.
> `spark.sql.shuffle.partitions` 를 2000 미만으로 낮추면 정상 통계가 나오고 skew 감지가 작동한다.

우리가 세운 P6 와 **동일한 건**이다. 자체 규칙("이미 있으면 버린다")에 따라 novelty 주장에서 제외한다.
계획돼 있던 **S4c(54 run)는 취소**한다.

부수적으로 알게 된 것: `spark.shuffle.accurateBlockSkewedFactor` 라는 설정이 존재한다.
Spark 가 이 문제를 이미 인지하고 대응 수단을 만들어 뒀다는 뜻이다.

→ 블로그에서는 **배경/보강 근거**로 SPARK-48290 을 인용한다. 발견으로 쓰지 않는다.

---

## P3 — 유지하되 표현을 낮춘다

### 소스로 확정된 것

`OptimizeSkewedJoin.scala` (apache/spark master) 실측:

```scala
val leftMedSize  = Utils.median(leftSizes, false)   // leftSizes = mapStats.bytesByPartitionId
val rightMedSize = Utils.median(rightSizes, false)

def getSkewThreshold(medianSize: Long): Long =
  conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
    (medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR)).toLong)

val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
```

**`bytesByPartitionId` 만 쓴다. row/record count 는 어디에도 없다.** 확정.

> 참고: 임계는 `max(256MB, 5 × median)` 형태다. 결과적으로 "256MB 초과 AND 5×median 초과"와 같지만,
> 코드는 AND 가 아니라 `max` 로 구현돼 있다. 문서에 그렇게 적어둘 것.

### 그런데 한계 자체는 이미 언급돼 있다

적어도 한 곳(cazpian.ai 의 skew 가이드)이 이렇게 쓰고 있다:

> AQE offers no guarantees when the skew is driven by in-memory amplification or
> per-row computational complexity rather than raw byte volume.

즉 **"AQE 가 바이트만 본다"는 지적 자체는 새롭지 않다.**

### 그래서 우리가 주장할 수 있는 것

- ❌ "AQE 가 record-skew 를 못 본다는 것을 발견했다"
- ✅ **"그것을 측정했다."** 구체적으로:
  - byte-skew 3/3 감지 vs record-skew **0/6 감지** (플랜 마커 `skew=true` / `coalesced and skewed` 기준)
  - 사각지대의 비용: R=64 에서 1,274ms → **3,481ms (2.73배)**, AQE 는 무반응
  - **AQE 가 구제한 파티션(1,618ms)보다 무시한 파티션(3,634ms)이 2.2배 느리다**
  - 왜 그런지의 정량 근거: `1741 ns/record vs 4.95 ns/byte` (S2)

SPARK-29544 의 설명문에는 "runtime statistics (data size **and row count**)" 라고 적혀 있으나,
**실제 머지된 코드에는 row count 가 없다.** 설계 의도와 구현이 갈린 흔적으로 보인다.
이건 JIRA 코멘트로 물어볼 가치가 있다.

---

## P4 — 표현을 대폭 낮춘다

1/2n ~ 1/n 메모리 분배는 공식 문서·튜닝 블로그에 널리 문서화돼 있고, **예측까지 이미 쓰여 있다**:

> more cores per executor increases task concurrency, which reduces per-task memory
> allocation, potentially triggering earlier spill thresholds and exacerbating data skew effects.

어제 "novelty 5점"으로 평가했던 것은 **과대평가였다.** 실제로는:

- ❌ "cores-per-executor 가 skew 내성을 결정한다는 것을 발견했다"
- ✅ "널리 예측돼 있지만 측정된 적 없는 것을, **cliff 위치의 이동량으로** 정량화했다"

S3 는 계속 진행하되 **블로그에서의 비중을 낮춘다.** 단독 꼭지가 아니라 P2/P3 를 보강하는 근거로 쓴다.

---

## 이 재확인에서 배운 것

1. **"블로그에 없다"와 "JIRA에 없다"는 다르다.** P6 는 웹 검색 6개로는 안 나왔고 JIRA 를 직접 뒤져서 나왔다.
2. **JIRA 설명문과 머지된 코드가 다를 수 있다** (SPARK-29544). 최종 근거는 소스다.
3. **어제의 novelty 평가는 낙관적이었다.** 6개 중 1개 폐기, 2개 하향. 실험 전에 했으면 S4c 54 run 을 아꼈을 것이다.
   → **다음 프로젝트에서는 실험 설계 직후, 실행 전에 문헌 확인을 넣는다.**

## 참고

- [SPARK-48290 AQE not working when joining dataframes with more than 2000 partitions](https://issues.apache.org/jira/browse/SPARK-48290)
- [SPARK-29544 Optimize skewed join at runtime with new Adaptive Execution](https://issues.apache.org/jira/browse/SPARK-29544)
- [SPARK-31864 Adjust AQE skew join trigger condition](https://issues.apache.org/jira/browse/SPARK-31864)
- [SPARK-20801 Store accurate size of blocks in MapStatus when it's above threshold](https://issues.apache.org/jira/browse/SPARK-20801)
- [OptimizeSkewedJoin.scala (apache/spark master)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala)
