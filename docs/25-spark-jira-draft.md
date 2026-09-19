# 25. Apache Spark JIRA 초안 (P3)

작성: 2026-09-19 · 제출 **전** 단계. 아래 §0 체크리스트를 통과해야 올린다.

---

## 0. 올리기 전 확인한 것 / 확인할 것

| | 상태 |
|---|---|
| 기존 JIRA 중복 검색 | ✅ 없음. 가장 가까운 것은 SPARK-59436(SPJ) 인데 **방향이 반대**다 — 거긴 바이트 통계가 없어서 split 개수를 쓴다 |
| `MapOutputStatistics` 에 레코드 수가 있나 | ✅ **없다.** 소스 확인: `shuffleId`, `bytesByPartitionId` 둘뿐 |
| `recordsByPartitionId` 라는 게 있던데? | ⚠️ 그건 **map task 당** 레코드 수지 reduce 파티션당이 아니다 |
| 톤 | ⚠️ "버그 발견" 아님. **Improvement + 측정 근거 제공**. `docs/04` 의 결론을 지킨다 |
| 남은 일 | ⬜ `dev@spark.apache.org` 에 먼저 물어보는 편이 낫다 (§5) |

> `docs/04-novelty-risk.md` 의 판정: *"❌ 새로운 사실을 발견했다 / ✅ 알려질 수 있었지만
> 아무도 측정해서 보여주지 않은 것을 측정했다"*. 이 톤을 벗어나면 안 된다.

---

## 1. 제목

```
AQE skew join detection is byte-only; record-heavy partitions are never split
```

**Type**: Improvement (Bug 아님)
**Component**: SQL
**Affects Version**: 4.0.1 (measured), 3.x (code path unchanged)

---

## 2. 본문 초안 (영문)

```text
h2. Summary

AQE's skew join detection uses partition size in bytes only. A partition that is
average-sized in bytes but holds many more *records* than its peers is never
detected, and in my measurements that partition was 2.2x slower than the ones
AQE did rescue.

This is not a code oversight -- the statistic simply is not available. I am
filing this to (a) report the measurement and (b) ask whether making it
available is considered worthwhile.

h2. Where it comes from

OptimizeSkewedJoin decides via ShufflePartitionsUtil using
MapOutputStatistics, which carries only:

  class MapOutputStatistics(
      val shuffleId: Int,
      val bytesByPartitionId: Array[Long])

The predicate is:

  size > SKEW_JOIN_SKEWED_PARTITION_THRESHOLD (default 256MB)
    && size > median * SKEW_JOIN_SKEWED_PARTITION_FACTOR (default 5)

Both terms are bytes. There is no per-reduce-partition record count anywhere in
the statistics AQE sees. (MapStatus does not expose one either; the
record counts that exist are per map task, not per reduce partition.)

Worth noting: the description on SPARK-29544, which introduced this rule, says
the optimization is "based on the runtime statistics (data size *and row
count*)". The merged implementation uses data size only.

h2. Why it matters -- measurement

I decomposed the cost of a shuffle-and-sort on a skewed partition
(90 runs, EC2 m6id.4xlarge, Spark 4.0.1, single node):

  duration_ms = 1741 ns * records + 4.95 ns * bytes - 1602
  R^2 = 0.9958  (n = 90, 3 row widths x 6 skew levels x 5 reps)

One record costs ~352x one byte. With 64-byte rows, 88% of the cost comes from
the record term; with 1024-byte rows, 27%.

So bytes are the detection signal while records dominate the cost. That gap is
directly observable.

h2. Reproduction

Data generator: hot key gets narrow rows (64B), cold keys get wide rows (2048B).
Bytes stay near-uniform across partitions while record count skews up to 64x.
By construction this passes neither the 256MB threshold nor the 5x-median factor.

Result (AQE on, SortMergeJoin forced, 60 runs):

  byte-skew   : 3/3 detected. max task 19,164ms -> 1,127ms  (17x faster)
  record-skew : 0/6 detected. max task  3,481ms -> 3,634ms  (no change)

And the comparison that I think matters most:

  partition AQE rescued (byte-skew 8)   : 6,256ms -> 1,618ms
  partition AQE ignored (record-skew 64):  3,481ms ->  3,634ms

  The ignored partition ends up 2.2x slower than the rescued one.

AQE is not underperforming here. On byte skew it is excellent -- 17x. It simply
cannot see this case.

Detection was confirmed from the event log plan markers, not inferred from task
counts: "SortMergeJoin ... skew=true" and "AQEShuffleRead coalesced and skewed"
on Spark 4.0.

h2. What I checked before filing

* Lowering spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes does not
  help. I tried 32MB: partitions do get split (split=true, hot partition drops
  from 189.7MB to 60.9MB), but wall clock moved 1.03-1.07x with a repeat spread
  of 0.20-0.33s. Splitting a partition that already fits the execution memory
  pool buys nothing. The default 256MB looked well chosen in my setup.
  => this is not a threshold-tuning problem.

* Raising spark.sql.shuffle.partitions does not help either (200 -> 2000 moved
  the hot partition by 2.6%): one hot key lands in one partition regardless.

h2. Possible directions (I do not have a strong opinion)

1. Carry per-reduce-partition record counts in MapOutputStatistics and add an
   OR term to the predicate. The obvious cost is driver memory and the
   interaction with HighlyCompressedMapStatus -- which is exactly why I am
   asking rather than proposing a patch.

2. Leave detection as is, but document the limitation in
   sql-performance-tuning.md so users know to check row counts when AQE is on
   and a straggler remains.

Option 2 alone would already have saved me a lot of time.

h2. Limitations of my measurement

* Single node (local[N]); no cross-node network shuffle.
* 8 GiB datasets, 200 shuffle partitions, one executor configuration.
* The 1741 ns/record coefficient is a local fit in the region where spilling
  occurs. Extrapolated to small partitions it overpredicts by ~60%; I verified
  this separately. The structural claim (records dominate for narrow rows) is
  what I would stand behind, not the constant.

Harness, raw per-task data, figures, and the record of the things I got wrong:
https://github.com/JeonDaehong/spark-skew-test
```

---

## 3. 첨부

| 파일 | 용도 |
|---|---|
| `results/s4b/figures/aqe_blindspot.png` | byte 3/3 vs record 0/6 |
| `results/s2_ec2/figures/decompose.png` | 비용 분해 (R²=0.9958) |

---

## 4. 쓰면서 지킨 것 / 피한 것

**지킨 것**

- "버그"라고 안 썼다. `MapOutputStatistics` 에 레코드 수가 **없다**는 걸 먼저 밝혔다.
  조건 추가가 아니라 **통계 배관** 문제라는 게 정확한 성격이다.
- AQE 가 byte-skew 에서 **17배** 개선한다는 사실을 같이 적었다. 안 적으면
  "AQE 가 못 쓴다" 로 읽힌다.
- 임계값 조정으로 안 된다는 것을 **먼저 시험하고** 그 결과를 넣었다.
  안 그러면 첫 답글이 "threshold 낮춰보세요" 로 끝난다.
- 한계(단일 노드, 계수의 국소성)를 내가 먼저 적었다.
- 해결책을 밀지 않고 **선택지 2개를 던지고 물었다.** 2번(문서화)만으로도 값어치가 있다.

**피한 것**

- 패치 제안. `HighlyCompressedMapStatus` 와의 상호작용을 이해 못 한 채로 PR 을
  올리면 안 된다.
- "아무도 몰랐던 발견". 소스를 읽으면 보이는 것이고 커미터에겐 자명할 수 있다.
- SPARK-48290 을 근거로 끌어오기. **별개 건**이다 (2000 파티션 초과 문제).

---

## 5. 제출 경로 — JIRA 보다 dev@ 가 먼저다

`docs/04` 의 판정이 "커미터에겐 자명할 수 있음"이었다. 그렇다면 JIRA 에 바로
올리는 것보다 `dev@spark.apache.org` 에 **짧게 물어보는 쪽**이 낫다.

```text
Subject: [DISCUSS] AQE skew join detection is byte-only -- is record count worth carrying?

(본문: §2 의 Summary + Why it matters + 선택지 2개, 5~6 문단으로 압축.
 전체 측정은 링크로.)
```

이유:

1. 이미 논의된 적이 있다면 **JIRA 를 더럽히지 않고** 알 수 있다.
2. "통계를 추가하자"는 설계 결정이라 JIRA 티켓보다 메일 스레드에 맞다.
3. 반응이 있으면 그때 JIRA 로 옮기면 된다.

⬜ **다음 행동**: dev@ 메일 먼저. 반응 보고 JIRA.

---

## 6. 아직 안 한 것

- `dev@spark.apache.org` 아카이브 **전수** 검색 (웹 검색만 했다)
- `HighlyCompressedMapStatus` 가 레코드 수를 어떻게 다룰지 코드 레벨 검토
- 멀티 노드 재현 — 이게 없으면 "단일 노드 얘기 아니냐"는 반론에 답을 못 한다
