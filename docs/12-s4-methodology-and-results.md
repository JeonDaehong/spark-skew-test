# 12. S4 — AQE는 record-skew를 보는가: 방법론과 결과

실행: 2026-09-13, EC2 m6id.4xlarge
- **S4** (P=200): 54 run · 21분 — 설계 한계 발견
- **S4b** (P=50): 60 run · 26분 — 본 결과 (4 run OOM 실패)

원자료: `results/s4/`, `results/s4b/` · 그림: `results/s4b/figures/aqe_blindspot.png`

---

## 결론 먼저

> **AQE는 record-skew를 한 번도 감지하지 못했다. 그리고 그 사각지대에는 실제 비용이 있다.**
>
> ```
> byte-skew 8    AQE가 쪼갬   6,256ms → 1,618ms   (3.9배 개선)
> record R=64    AQE 방치      3,481ms → 3,634ms   (변화 없음)
> ```
>
> **AQE가 구제한 파티션(1,618ms)보다 무시한 파티션(3,634ms)이 2.2배 느리다.**

AQE의 능력이 부족해서가 아니다. byte-skew에서는 압도적으로 잘 작동한다 — skew 32에서 **19,164ms → 1,127ms, 17배**. 단지 **보지를 못한다.**

**명제 P3 → 참.**

---

## 1. 질문

`docs/00-research-triage.md`의 P3:

> **AQE는 record-skew를 감지하지 못한다** (판정이 byte 전용이므로)

S2가 이 질문을 최우선으로 끌어올렸다. AQE의 skew 판정은 `ShufflePartitionsUtil`에서 `bytesByPartitionId`만 본다 — 바이트만. 그런데 S2에서 비용의 지배 요인은 레코드였다 (좁은 행에서 88%).

⇒ **바이트로는 평범한데 레코드가 몰린 파티션**은 감지되지 않으면서 실제로는 비쌀 것이다.

## 2. 설계

### 2군 비교

| | 구성 | AQE 발동 조건 |
|---|---|---|
| **A. byte-skew** | 행 폭 고정(256B), hot key에 행을 몰아줌 | `>256MB` AND `>5×median` → **충족** |
| **B. record-skew** | hot key는 좁은 행(64B), cold key는 넓은 행(2048B) | **구조적으로 미달** |

### record-skew 데이터의 수식

```
record skew  R    = 1 + n_hot / m          (m = median 파티션 행 수)
byte   skew  beta = 1 + (R-1) × w_hot / w_cold

w_hot=64, w_cold=2048 → R=64 에서도 beta = 2.6  (AQE의 5× 조건 미달)
```

행 수 역산: `N_cold = B / (bpr_cold + (R-1)·bpr_hot/P)`, `n_hot = (R-1)·N_cold/P`
두 행 폭 각각 실측 캘리브레이션 (`gen/make_skewed.py --skew-mode record`).

### 워크로드를 join으로 바꾼 이유

AQE의 `skewJoin`은 **SortMergeJoin에만** 적용된다. S1/S2의 `repartition + sort`로는 skew 분할이 일어나지 않아 검증 자체가 불가능하다.
→ 팩트 × 디멘션 join, `autoBroadcastJoinThreshold=-1`로 SMJ 강제. 플랜에서 `SortMergeJoin Inner` 확인.

### Positive control이 먼저다

B군의 "미발동"이 의미를 가지려면 A군에서 AQE가 **실제로 발동**해야 한다. 이걸 먼저 확인하지 않으면 "AQE 설정이 잘못돼서 아무데서도 안 걸린 것"과 구분할 수 없다.

---

## 3. 감지 여부는 어떻게 판정했나 — task 수로 추정하면 안 된다

AQE는 coalesce로 task를 **줄이기도** 한다. task 수 변화만 보면 분할과 병합을 구분할 수 없다.
대신 실행 플랜에서 Spark가 직접 표시하는 마커를 읽는다 (`parse/parse_eventlog.py`의 `parse_aqe`).

**Spark 4.0 실측 마커:**

| 마커 | 의미 |
|---|---|
| `SortMergeJoin ... skew=true)` | 조인이 skew join으로 재작성됨 |
| `AQEShuffleRead coalesced and skewed` | shuffle read가 skew 파티션을 분할함 |
| `AQEShuffleRead coalesced` | 병합만 함 (**skew 처리 아님**) |

> ### ⚠️ 여기서 큰 실수를 할 뻔했다
>
> 처음에 `isSkewJoin=true`를 찾도록 짰다. 문헌·블로그에서 흔히 보이는 표기다.
> **Spark 4.0은 `skew=true`를 쓴다.** 그대로 뒀으면 A군에서도 "미감지"가 나와
> **"AQE는 skew를 전혀 처리하지 못한다"는 정반대 결론**이 나왔을 것이다.
>
> **교훈: 버전별 마커는 추측하지 말고 실제 로그에서 확인한다.**
> ```bash
> grep -oiE '[A-Za-z ]{0,28}skew[A-Za-z ]{0,28}[=:][^,"]{0,18}' <eventlog> | sort | uniq -c
> ```
> 이 한 줄이 결론을 뒤집었다.

---

## 4. S4 (P=200) — 설계 한계를 발견하고 폐기

A군은 완벽하게 작동했다 (skew 32에서 6,689ms → 555ms, 12배).
**그런데 B군은 비용이 전혀 오르지 않았다** — max task가 R=1이든 R=32든 261~282ms로 평탄.

원인은 설계였다:

```
byte skew를 낮추려면 cold 행을 넓게(2048B) 잡아야 한다
  → 총 레코드 수가 4.5M로 쪼그라든다
  → hot 파티션이 R=32에서도 0.7M 레코드뿐
  → 태스크 고정 오버헤드(~250ms)에 묻혀 per-record 비용이 안 드러난다
```

**덤으로 S2 모델의 한계도 드러났다.** 1741 ns/record는 0.7M 레코드에 대해 1.16초를 예측하는데 실제는 282ms였다. 그 계수는 **spill이 일어나는 큰 파티션 영역에서 적합된 값**이고 작은 파티션에 외삽하면 안 된다. S2 문서에 "국소 선형화"라고 적어둔 게 맞았다.

→ **파티션 수를 200 → 50으로 낮춰** 파티션당 크기를 4배로 키우고 재실행 (S4b).

---

## 5. S4b (P=50) — 본 결과

### AQE 감지 여부

| 군 | level | 감지 | `skew=true` | `and skewed` | byte skew | hot MB | hot Mrec |
|---|---:|---|---|---|---:|---:|---:|
| byte | 1 | ✗ | ✗ | ✗ | 1.32 | 205 | 0.92 |
| byte | 8 | **✅** | ✅ | ✅ | 1.35 | 180 | 0.80 |
| byte | 16 | **✅** | ✅ | ✅ | 1.38 | 158 | 0.71 |
| byte | 32 | **✅** | ✅ | ✅ | 1.24 | 127 | 0.57 |
| record | 1 | ✗ | ✗ | ✗ | 1.32 | 203 | 0.11 |
| record | 4 | ✗ | ✗ | ✗ | 1.32 | 203 | 0.33 |
| record | 8 | ✗ | ✗ | ✗ | 1.31 | 203 | 0.66 |
| record | 16 | ✗ | ✗ | ✗ | 1.31 | 202 | 1.32 |
| record | 32 | ✗ | ✗ | ✗ | 1.70 | 260 | 2.63 |
| record | 64 | ✗ | ✗ | ✗ | 2.51 | 377 | **5.18** |

**byte-skew 3/3 감지, record-skew 0/6 감지.**
(byte 군의 hotMB가 작은 것은 AQE on 기준이라 이미 분할된 뒤 값이기 때문)

### AQE 구제 효과

| 군 | level | hot MB off→on | 비율 | ms off→on | **비율** |
|---|---:|---|---:|---|---:|
| byte | 1 | 205 → 205 | 1.00 | 1,747 → 1,762 | 1.01 |
| byte | 8 | 1,065 → 180 | **0.17** | 6,256 → 1,618 | **0.26** |
| byte | 16 | 1,875 → 158 | **0.08** | 11,646 → 1,384 | **0.12** |
| byte | 32 | 3,016 → 127 | **0.04** | 19,164 → 1,127 | **0.06** |
| record | 1 | 203 → 203 | 1.00 | 1,274 → 1,257 | 0.99 |
| record | 8 | 203 → 203 | 1.00 | 1,271 → 1,199 | 0.94 |
| record | 16 | 202 → 202 | 1.00 | 1,341 → 1,359 | 1.01 |
| record | 32 | 260 → 260 | 1.00 | 2,190 → 2,268 | 1.04 |
| record | 64 | 377 → 377 | 1.00 | 3,481 → 3,634 | 1.04 |

record 군은 hot 파티션 크기가 **한 MB도 변하지 않는다.** AQE가 손을 대지 않았다는 뜻이다.

### 사각지대에 비용이 있는가 — 있다

record 군, AQE off 기준:

| R | hot Mrec | hot MB | ms | IQR | **vs R=1** |
|---:|---:|---:|---:|---:|---:|
| 1 | 0.11 | 203 | 1,274 | 48.5 | 1.00 |
| 4 | 0.33 | 203 | 1,236 | 19.5 | 0.97 |
| 8 | 0.66 | 203 | 1,271 | 14.0 | 1.00 |
| 16 | 1.32 | 202 | 1,341 | 21.0 | 1.05 |
| **32** | 2.63 | 260 | **2,190** | 103.5 | **1.72** |
| **64** | 5.18 | 377 | **3,481** | 167.0 | **2.73** |

R=16까지는 평탄하다가 R=32부터 오른다. 증가폭(849ms, 1,291ms)이 IQR(103, 167)의 8배 이상이라 노이즈가 아니다.

---

## 6. 정직한 단서

### (a) R=64에서 byte skew가 2.51까지 올라갔다

hot 파티션이 203MB → 377MB가 됐다. 즉 **순수 record 효과만은 아니다.**
R=16→64에서 바이트 ×1.87, 레코드 ×3.92, 시간 ×2.60 — 두 요인 사이에 있다.

다만 **AQE의 5× 조건은 여전히 미달**이므로 "AQE가 못 본다"는 결론은 그대로 유효하다.
순수 record 효과를 분리하려면 더 넓은 cold 행(4096B+)이 필요하고, 그러면 다시 절대 레코드 수가 줄어든다 — §4의 긴장이 반복된다. **이 설계로는 둘 다 최대화할 수 없다.**

### (b) 4/60 run이 OOM으로 실패했다

P=50이라 파티션이 4배 커졌는데 driver를 1500m 그대로 뒀다. 메모리 경계에서 돈 설정이다.
실패한 셀: `byt16/aqe1/rep2`, `rec1/aqe0/rep0`, `rec32/aqe1/rep2`, `rec64/aqe1/rep2` → 해당 셀은 3회가 아닌 2회 반복.
**핵심 주장이 걸린 셀(rec32·rec64의 AQE off)은 3회 온전하다.**

다만 OOM 직전까지 간 run들은 GC 부담으로 성능이 왜곡됐을 수 있다. **재실행한다면 P=50에서는 exec-mem을 3g로 올려야 한다.**

### (c) Spark 4.0 버그: OOM이 `FAILED_READ_FILE`로 둔갑한다

```
org.apache.spark.SparkException: [FAILED_READ_FILE.NO_HINT] ... .parquet
  Caused by: scala.MatchError: java.lang.OutOfMemoryError: Java heap space
    at org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2$.attachFilePath(FileDataSourceV2.scala:127)
```

`attachFilePath`가 OOM을 처리하지 못해 `MatchError`를 내고, 그 결과 **"파일 읽기 실패"**로 표시된다. 실제로는 메모리 부족이다. parquet 파일은 멀쩡했다 (PAR1 매직 정상).

진단을 크게 방해하는 에러 메시지 품질 문제다. **JIRA 후보** (P3와 별개 건).

---

## 7. 한계

1. 단일 환경·단일 워크로드(join). 8 GiB · P=50 · 1500m 한 점.
2. §6-(a) — record와 byte 효과가 고점에서 완전히 분리되지 않았다.
3. §6-(b) — 메모리 경계 설정. 재실행 시 3g 권장.
4. AQE 임계값은 기본값(256MB / 5×)만 썼다. 임계를 낮추면 감지되는지는 확인 안 했다 — **후속 실험 후보**: `skewedPartitionFactor`를 2로 낮춰도 record-skew는 여전히 안 잡히는가?
5. "AQE가 byte 기반"이라는 것 자체는 소스를 읽으면 보인다. 정직한 표현은 *"새로 발견했다"*가 아니라 ***"알려질 수 있었지만 아무도 측정해서 보여주지 않은 것을 측정했다"***.

---

## 8. 다음

| 항목 | 내용 |
|---|---|
| **문헌 재확인 (필수)** | P3를 Spark JIRA·dev@ 메일링·GitHub 이슈에서 전수 검색. 이미 보고된 건이면 novelty 주장을 내린다 |
| 후속 실험 | `skewedPartitionFactor` 낮춰도 안 잡히는가 (§7-4) |
| 재실행 | P=50 + exec-mem 3g 로 OOM 제거 |
| JIRA 후보 2건 | ① AQE skew 판정이 record를 보지 않음 ② OOM이 `FAILED_READ_FILE`로 표시됨 |

## 블로그 가치

- ❌ "AQE로 skew를 해결하세요" — 어디에나 있다
- ✅ **"AQE는 바이트만 본다. 레코드가 몰린 파티션은 손도 안 댄다."**
- ✅ **"AQE가 구제한 파티션보다 무시한 파티션이 2.2배 느렸다."**
- ✅ 진단 조언: **AQE를 켰는데도 straggler가 남으면 파티션의 _레코드 수_를 봐라.** `bytes`만 보면 안 보인다.
