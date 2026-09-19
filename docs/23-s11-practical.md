# 23. S11 / S11b — skew 를 무엇으로 막는가, 그리고 무엇이 안 듣는가

실행: 2026-09-19 · EC2 m6id.4xlarge (spot) · **108 run** (성공 104 / 설계상 실패 4)
스크립트: `runner/s11_sweep.sh`, `runner/s11b_sweep.sh` · 그림: `results/s11/figures/practical.png`

---

## 0. 요약

| 처방 | 판정 |
|---|---|
| **broadcast join** | ✅ **286 MiB 까지 2.4배.** 286~572 MiB 사이에서 OOM |
| **AQE** | ✅ skew 32 에서 **1.20배**. 단 hot 이 256MB 를 넘어야 작동한다 |
| **hot key 분리** | ✅ **1.22배. 가장 빠르고 가장 안정적** (퍼짐 0.11s) |
| **salting (dim ×16)** | ❌ **오히려 느리다** (0.82~0.94배). dim 복제 비용이 이득을 먹는다 |
| **파티션 수 늘리기** | ❌ **효과 없음.** hot 파티션이 525.9 → 512.0 MiB (2.6%) |
| **AQE 임계값 낮추기** | ⚠️ 쪼개지긴 하는데 **시간을 안 준다** (1.03~1.07배, 산포 0.2~0.33s) |
| **NULL key** | inner 는 Spark 가 알아서 턴다. **left outer 는 못 턴다 (hot 59.4배)** |

## 1. 왜 했나

블로그 앞부분에 "skew 는 어디서 생기고 무엇으로 막는가"를 넣으려는데, 기존 실험에
**없는 것**이 많았다. 측정 없이 일반론으로 쓰면 이 프로젝트의 축("네 개 예상 중
셋이 틀렸다")과 결이 안 맞는다. 그래서 전부 쟀다.

이미 가지고 있던 것은 다시 안 쟀다.

- AQE 는 byte-skew 를 3/3 쪼개고 record-skew 를 0/6 놓친다 (S4/S4b)
- record R=64 는 hot 376.9 MiB 로 **256MB 임계를 넘는데도** 안 쪼갠다
  → 두 번째 조건(중앙값의 5배)에서 걸러진다
- AQE 가 분할이 아니라 **병합**만 하는 경우도 있다 (record R=1: 135.6 → 203.4 MiB)

## 2. broadcast join 의 경계

dim 을 키 개수로 키웠다. fact 의 키 범위를 넘는 행은 매칭되지 않는데, 그게 현실이다 —
디멘션은 보통 참조되는 것보다 크다. broadcast 는 `F.broadcast()` 힌트로 **강제**했다.

| dim 크기 | broadcast | SortMergeJoin |
|---:|---|---|
| 1.9 MiB | **6.5초** | 15.6초 |
| 95.4 MiB | **7.2초** | 15.5초 |
| 286.1 MiB | **8.7초** | 15.9초 |
| 572.2 MiB | **실패 2/2** | 15.9초 |
| 953.7 MiB | **실패 2/2** | 16.5초 |

```
org.apache.spark.SparkException:
  Not enough memory to build and broadcast the table to all worker nodes
    at BroadcastExchangeExec.doExecuteBroadcast
```

**286 MiB 까지는 2.4배 빠르고, 286~572 MiB 사이 어딘가에서 죽는다.**
SMJ 는 953.7 MiB dim 도 16.5초에 멀쩡히 끝낸다 — 느리지만 안 죽는다.

driver 힙이 1500m 다. 경계가 힙의 약 20~38% 사이에 있는데, 정확한 지점은
이 격자(5점)로는 못 좁혔다.

## 3. AQE 가 손대지 않는 구간 — 그런데 손댈 필요도 없었다

AQE 의 판정은 **두 조건의 AND** 다 (`OptimizeSkewedJoin.scala`).

```
size > skewedPartitionThresholdInBytes (기본 256MB)
  AND size > median × skewedPartitionFactor (기본 5)
```

총 데이터를 1 GiB 로 줄여 hot 파티션이 256MB 를 못 넘게 만들었다.

| skew | hot (AQE off) | hot (AQE on) | wall off / on | split |
|---:|---:|---:|---|---|
| 1 | 6.9 MiB | 60.4 MiB | 7.74 / 7.62 s | False |
| 8 | 36.2 MiB | 61.0 MiB | 7.85 / 7.55 s | False |
| 20 | 86.1 MiB | 86.1 MiB | 7.99 / 7.54 s | False |
| 50 | **189.7 MiB** | 189.7 MiB | 8.11 / 7.81 s | **False** |

**skew 50 에서도 AQE 는 손을 안 댄다.** 첫 조건(256MB)에 미달이기 때문이다.

> ⚠️ **그런데 여기가 내가 틀린 지점이다.**
> "AQE 에 사각지대가 있다"를 보이려고 만든 실험인데, **느려지지도 않았다**
> (7.74 → 8.11초). 이유는 간단하다. hot 이 189.7 MiB 라 **실행 풀(720 MiB)에
> 한참 못 미쳐 계단을 안 밟는다.**

(skew 1·8 에서 AQE 가 hot 을 **키운** 것도 눈에 띈다: 6.9 → 60.4 MiB.
분할이 아니라 병합이다. S4 에서 본 것과 같은 현상.)

### 임계값을 낮추면?

그래서 `skewedPartitionThresholdInBytes` 를 32 MiB 로 낮춰봤다.

| skew | arm | wall | hot | split | 반복 산포 |
|---:|---|---:|---:|---|---:|
| 20 | AQE off | 7.88s | 86.1 MiB | — | |
| 20 | AQE 256MB | 7.62s | 86.1 MiB | False | |
| 20 | **AQE 32MB** | 7.65s | **60.5 MiB** | **True** | 0.33s |
| 50 | AQE off | 8.12s | 189.7 MiB | — | |
| 50 | AQE 256MB | 7.87s | 189.7 MiB | False | |
| 50 | **AQE 32MB** | 7.59s | **60.9 MiB** | **True** | 0.20s |

**쪼개지긴 한다** (split=True, hot 이 60 MiB 대로 내려감). **그런데 시간을 안 준다.**
1.03~1.07배인데 반복 산포가 0.20~0.33초다. skew 50 의 0.53초 차이는 산포보다 크지만
의미 있는 크기는 아니다.

> 💡 **256MB 기본값은 이 설정에서 합리적이다.**
> 실행 풀(720 MiB)보다 작은 파티션을 쪼개봐야 오버헤드만 늘고 얻을 게 없다.
> **AQE 의 진짜 사각지대는 "크기"가 아니라 "레코드"다** (S4).
>
> 다만 이건 **풀 크기에 달려 있다.** executor memory 를 700m 로 낮추면
> 풀이 240 MiB 가 되어 256MB 임계보다 작아진다. 그때는 임계값을 낮추는 게
> 의미가 있을 수 있다. 여기서는 확인하지 않았다.

## 4. 무엇이 실제로 버는가

4 GiB, 네 팔을 같은 조건에서 나란히.

| skew | 처방 | wall | hot read | spill | vs 무처리 | 반복 퍼짐 |
|---:|---|---:|---:|---:|---:|---:|
| 8 | 무처리 | 15.46s | 145.4 MiB | 0.0 | 1.00× | 0.29s |
| 8 | AQE | 15.80s | 145.4 MiB | 115.7 MiB | **0.98×** | 0.27s |
| 8 | salting ×16 | 18.96s | 28.8 MiB | 0.0 | **0.82×** | 0.49s |
| 8 | hot key 분리 | 15.26s | 26.8 MiB | 0.0 | 1.01× | **0.02s** |
| 32 | 무처리 | 18.20s | 525.9 MiB | 462.8 MiB | 1.00× | 0.40s |
| 32 | AQE | 15.21s | 60.6 MiB | 0.0 | **1.20×** | 0.16s |
| 32 | salting ×16 | 19.32s | 50.5 MiB | 0.0 | **0.94×** | 0.91s |
| 32 | **hot key 분리** | **14.87s** | 24.0 MiB | 0.0 | **1.22×** | 0.11s |

### salting 이 진다

hot 파티션은 확실히 줄었다 (525.9 → 50.5 MiB). **그런데 시간은 늘었다.**
dim 을 16배로 복제하는 비용이 이득을 전부 먹었다.

> ⚠️ **공정하게 적는다.** 내 구현은 **모든 키에** salt 를 붙였고 dim 을 통째로
> 16배 복제했다. hot key 에만 salt 를 붙이는 영리한 구현도 있다 — 그건 사실상
> 아래의 "hot key 분리"와 같은 아이디어다. 여기서 진 것은 **소박한 salting** 이다.

### hot key 분리가 이긴다

hot key 만 떼어 broadcast join 하고 나머지는 SMJ, 둘을 union 한다.
**hot 쪽 dim 은 한 행뿐이라 broadcast 가 공짜다.** salting 처럼 dim 을 복제하지 않는다.

skew 32 에서 1.22배로 가장 빠르고, 반복 퍼짐이 0.11초로 가장 안정적이다.
AQE 가 작동하지 않는 skew 8 에서도 손해가 없다 (1.01배).

### AQE 는 작동할 때만 번다

skew 32 에서는 1.20배로 좋다. 그런데 skew 8 (hot 145.4 MiB < 256MB) 에서는
**0.98배로 약간 손해**고, spill 이 0 → 115.7 MiB 로 **생겼다**. 병합이 만든
더 큰 파티션이 풀을 넘긴 것으로 보인다 — 확인하지 않았다.

## 5. 파티션 수 늘리기는 효과가 없다 (음성 대조군)

skew 를 만나면 제일 먼저 `spark.sql.shuffle.partitions` 를 올린다. 4 GiB, skew 32.

| partitions | wall | hot read | spill | n_tasks |
|---:|---:|---:|---:|---:|
| 200 | 18.22s | 525.9 MiB | 462.8 MiB | 249 |
| 800 | 16.78s | 513.9 MiB | 250.4 MiB | 849 |
| 2000 | 17.84s | **512.0 MiB** | 131.2 MiB | 2049 |

**hot 파티션이 2.6% 밖에 안 줄었다.** 파티션을 10배로 늘려도 **키 하나에 몰린 행은
여전히 한 파티션**이다. 해시가 그 키를 어디로 보내든 한 곳이다.

spill 총량은 462.8 → 131.2 MiB 로 줄었는데, 이건 hot 이 아니라 **나머지 파티션들이
작아져서**다. 그리고 wall 은 안 줄었다 — hot 태스크가 지배하기 때문이다.
2000 에서 오히려 다시 늘어난 것은 태스크 오버헤드로 보인다.

예상대로 나왔다. **이 팔이 예상과 달랐다면 "hot 파티션 하나가 지배한다"는 이
프로젝트 전체의 해석이 흔들렸을 것이다.**

## 6. NULL key — inner 는 공짜, outer 는 직격

해시 파티셔닝에서 NULL 은 전부 같은 파티션으로 간다. hot key 와 같은 구조다.
4 GiB 중 **30% 를 NULL key** 로 만들고 inner 와 left outer 를 비교했다.

| | wall | hot read | spill |
|---|---:|---:|---:|
| inner join | **12.42s** | **19.4 MiB** | 0.0 |
| left outer join | 18.95s | **1,155.6 MiB** | 978.2 MiB |

**hot 파티션이 59.4배, 시간이 1.53배 차이난다.**

이유는 실행 플랜에 그대로 찍혀 있다 (추정이 아니다).

```
inner:
  PushedFilters: [IsNotNull(key)]
  (3) Filter [codegen id : 1]  Condition : isnotnull(key#0)
  (4) Exchange                              ← 필터가 셔플 앞에 있다

outer:
  Scan parquet   (PushedFilters 없음)
  (3) Exchange                              ← 필터 없이 바로 셔플
```

inner join 은 NULL 행이 결과에 못 들어가므로 Spark 가 `IsNotNull` 을 추론해
**parquet 스캔까지 밀어넣는다** (`InferFiltersFromConstraints`). left outer 는
NULL 행도 결과에 남아야 하므로 그 최적화가 **원리적으로 막힌다.**

> 💡 **"NULL 때문에 느려요"는 inner join 에서는 거의 틀린 말이다.**
> outer join 이나 group by 에서만 문제가 된다. 그때는 NULL 을 따로 처리해야 한다.

## 7. 판정

| | |
|---|---|
| broadcast 경계 | ✅ 286 MiB 성공 / 572 MiB 실패 (2/2). driver 1500m |
| AQE 유효 구간 | ✅ hot > 256MB 일 때만. skew 50 도 189.7 MiB 면 무반응 |
| AQE 임계값 하향 | ⚠️ 분할은 되는데 **이득 없음**. 풀보다 작은 파티션이라 그렇다 |
| hot key 분리 | ✅ **1.22배, 퍼짐 0.11s — 최선** |
| salting | ❌ 0.82~0.94배. **단, 소박한 구현 기준** |
| 파티션 수 | ❌ hot 2.6% 감소. 음성 대조군 통과 |
| NULL key | ✅ inner/outer 가 플랜 레벨에서 갈린다 |
| 신뢰도 | 높음 (반복 퍼짐 대부분 0.5초 미만, 플랜 마커로 확인) |

## 8. 남은 구멍

1. **broadcast 경계를 286~572 MiB 로만 좁혔다.** 더 촘촘한 격자가 필요하다.
2. **AQE 임계값 하향은 풀이 큰 경우에만 확인했다.** executor memory 를 낮춰
   풀 < 256MB 를 만들면 결론이 달라질 수 있다. 안 해봤다.
3. **salting 은 소박한 구현이다.** hot key 에만 salt 를 붙이는 변형은 안 쟀다
   (그건 hot key 분리와 사실상 같다).
4. **NULL 은 join 만 봤다.** group by 에서의 NULL skew 는 안 쟀다.
5. 여전히 단일 노드다.

## 9. 재현

```bash
bash runner/s11_sweep.sh                  # 54 run, 약 25분
bash runner/s11b_sweep.sh                 # 54 run, 약 25분
python parse/parse_eventlog.py --tag s11
python parse/parse_eventlog.py --tag s11b
python analysis/plot_s11.py
```

> 그림을 만들 때 `pandas 3` 에서 `df.skew` 가 **컬럼이 아니라 `DataFrame.skew()`
> 메서드**로 잡혀 마스크가 조용히 전부 False 가 됐다. 빈 그래프가 나왔는데
> 범례만 멀쩡해서 데이터 문제로 착각하기 쉬웠다. 컬럼 이름이 메서드와 겹치면
> 반드시 `df["skew"]` 로 접근할 것.
