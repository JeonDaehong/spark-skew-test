# 02. 실험 하네스 — 구조와 사용법

## 설계 원칙

1. **run 하나 = CSV 한 줄.** 모든 분석과 플롯은 `results/<tag>/summary.csv` 하나에서 출발한다.
2. **Spark 메트릭과 커널 메트릭을 같은 run에서 같이 찍는다.** 나중에 합치려 하면 시계가 안 맞는다.
3. **실패도 결과다.** OOM으로 죽어도 `meta.json`에 `error`가 남고 CSV에 한 줄로 들어간다.
4. **재현성 장치를 하네스에 박아둔다.** run마다 `drop_caches`, run 전후 커널 카운터 delta 기록.

## 파이프라인

```
gen/make_skewed.py     데이터 생성 (skew degree × row width 독립 통제)
        ↓  parquet + _gen_meta.json
runner/run_one.py      run 1개 실행 + 커널 샘플링 + eventlog
        ↓  results/<tag>/<run_dir>/{meta.json, samples.csv, eventlog/}
parse/parse_eventlog.py  eventlog → task 메트릭 + run 요약
        ↓  tasks.csv, summary.json, results/<tag>/summary.csv
analysis/plot_cliff.py   그림
        ↓  results/<tag>/figures/*.png
```

## 파일별 역할

| 파일 | 역할 | 핵심 |
|---|---|---|
| `env/setup-wsl.sh` | 환경 구축 (멱등) | venv·데이터는 **ext4**, repo만 `/mnt/d` |
| `gen/make_skewed.py` | 데이터 생성 | `f = (S-1)/(P+S-1)` 로 hot key 비율 계산 |
| `runner/sampler.py` | 커널 타임시리즈 | `/proc/meminfo`, `/proc/pressure/*`, `/proc/vmstat`, `/proc/stat` |
| `runner/run_one.py` | run 1개 | drop_caches → 샘플러 → Spark → 메타 기록 |
| `runner/smoke.sh` | 하네스 검증 | 1GiB, ~5분. **본 실험 전 필수** |
| `runner/s1_sweep.sh` | S1 본 실험 | 8GiB × 7 skew × 5 rep |
| `parse/parse_eventlog.py` | eventlog 파싱 | reduce 스테이지 자동 식별 후 집계 |
| `analysis/show.py` | 터미널 표 | 빠른 확인용 |
| `analysis/plot_cliff.py` | 그림 3종 | cliff / mechanism / skew_check |

## 생성기 수식

전체 N rows 중 비율 `f`를 단일 hot key에 몰아준다. 파티션 수 `P`일 때 hot key는 정확히 한 파티션에 떨어지므로:

```
S = ((1-f)N/P + fN) / ((1-f)N/P) = 1 + f·P/(1-f)
⇒ f = (S-1) / (P + S - 1)
```

**검증 완료** — 목표 1/4/16/64 → 실측 1.44/3.86/15.7/63.3 (`results/smoke2/figures/skew_check.png`).

payload는 sha2 기반 고엔트로피, parquet은 기본 `uncompressed`. 압축이 걸리면 "총 bytes 고정"이라는 통제가 깨진다. 실측 바이트 오차 **-2.5~-3.1%**.

## 워크로드 선택이 중요한 이유

기본은 `sort`다.

- ❌ `groupBy().count()` — map-side partial aggregation이 hot key를 mapper당 1행으로 접어버려 **skew 효과 자체가 사라진다**. (`--workload count`로 대조군 실행 가능)
- ✅ `repartition(key) + sortWithinPartitions` — hot 파티션의 모든 레코드가 `UnsafeExternalSorter`를 통과한다. LongArray 성장 → execution memory 고갈 → spill → page cache → writeback 경로를 정확히 자극한다.
- `agg` (`collect_list`) — 비대수적이라 역시 접히지 않음. 나중에 join과 함께 일반화 검증용.

## S1 사이징 근거

```
per-task execution pool = (exec_mem - 300MB) × spark.memory.fraction(0.6)
한 task 는 다른 task 가 놀면 풀 전체를 가져갈 수 있으므로 상한은 풀 전체.
hot partition in-memory footprint ≈ shuffle_read_bytes × 1.3   (스모크 실측)

MEM=1500m → pool = (1500-300)×0.6 = 720MB
GB=8, P=200 → median partition = 41MB
spill 시작 S ≈ 720 / (41 × 1.3) ≈ 13
⇒ sweep(1..64) 한가운데에 cliff 이 오도록 설계됨
```

> 스모크(1GiB, 1g)에서 spill이 0이었던 이유가 이것이다. peak execution memory 310MB < pool 434MB.
> **한 task가 1/N만 쓴다고 가정하면 틀린다** — 이 사실 자체가 RQ2의 출발점이다.

## 실행

```bash
source ~/.spark-skew-env

# 하네스 검증 (필수, ~5분)
bash runner/smoke.sh

# S1
bash runner/s1_sweep.sh
# 파라미터 오버라이드 예시
GB=8 MEM=1500m CORES=4 REPS=5 TAG=s1 bash runner/s1_sweep.sh

# 결과 확인
python analysis/show.py --tag s1
python analysis/plot_cliff.py --tag s1
python analysis/plot_cliff.py --tag s1 --dark
```

## summary.csv 주요 컬럼

| 컬럼 | 의미 |
|---|---|
| `actual_size_skew` | 실측 skew (max/median shuffle read bytes) — **B1 검증** |
| `actual_time_skew` | max/median task duration |
| `task_ms_p50/p90/max` | reduce 스테이지 task duration 분포 — **cliff 곡선의 y축** |
| `spill_disk_total`, `spill_mem_total` | spill |
| `peak_exec_mem_max` | task 최대 execution memory |
| `remote_to_disk_total` | **200MB 초과 블록 fetch-to-disk (RQ7)** — Spark UI가 spill로 안 세는 I/O |
| `gc_ms_total`, `gc_ms_max_task` | JVM GC |
| `delta_psi_io_full_us` | run 동안 누적된 PSI io.full — **커널이 실제로 막은 시간** |
| `peak_dirty_kb` | page cache Dirty 최고치 |
| `delta_pgpgout_kb` | 실제 writeback 양 |

## 하네스를 쓰다 배운 것 (되풀이하지 말 것)

### 1. 버전별 마커는 추측하지 말고 실측한다
AQE 감지기를 처음에 `isSkewJoin=true` 로 짰다. 문헌·블로그에 흔한 표기다.
**Spark 4.0 은 `skew=true` 를 쓴다.** 그대로 뒀으면 "AQE 는 skew 를 전혀 처리하지
못한다"는 정반대 결론이 나왔을 것이다. 확인 방법:
```bash
grep -oiE '[A-Za-z ]{0,28}skew[A-Za-z ]{0,28}[=:][^,"]{0,18}' <eventlog> | sort | uniq -c
```

### 2. 통제 장치는 실패할 때 시끄러워야 한다
EC2 로 옮겼을 때 `drop_caches` 가 권한 부족으로 조용히 실패하고 있었다 (로컬은 root,
EC2 는 ubuntu). 61GB RAM 이면 데이터셋이 통째로 page cache 에 남아 **다른 머신과
비교 자체가 불가능해진다.** 지금은 실패하면 run 을 즉시 중단한다 (`--no-drop-caches` 로만 해제).

### 3. 파생 지표로 추정하지 말고 원천 신호를 읽는다
"AQE 가 쪼갰는가"를 task 수 변화로 추정하려 했는데, AQE 는 coalesce 로 task 를
**줄이기도** 한다. 분할과 병합을 구분할 수 없다. 실행 플랜의 마커를 직접 읽어야 한다.

### 4. 'spill > 0' 은 cliff 마커로 쓰면 안 된다
S1 에서 skew 4/8/16 이 전부 136.5MB 를 spill 하는 것처럼 보였다. 원인은 두 가지였다.

**(a) 표시 반올림 착시.** raw 값은 143,173,819 / 143,167,339 / 143,161,152 bytes 로
서로 다른데 MiB 로 반올림하니 전부 136.5 가 됐다. **집계표에서 유효자리를 줄이면
"같은 값"이라는 없는 패턴이 만들어진다.** 이상하게 일정한 값을 보면 raw 를 확인할 것.

**(b) 그래도 대략 일정한 건 사실이고, 이유는 따로 있다.** task 단위로 내려가 보면
spill 하는 task 는 **단 하나**(hot 파티션, index 191)이고, 그 크기는 파티션 크기가
아니라 **sorter 가 첫 할당 실패를 맞은 시점에 쌓아둔 양**으로 정해진다. 그래서
shuffle read 가 283 -> 547 MiB 로 두 배가 되는 동안 spill 은 143 -> 136 MiB 로
거의 그대로다. skew 32 이상에서 여러 번 spill 하기 시작하면 그때부터 데이터에 비례한다.

올바른 마커는 **peak execution memory 의 포화**다. spill 양은 다회 spill 이
시작되기 전까지 파티션 크기를 대변하지 못한다.

### 5. 총 바이트 통제는 반드시 실측 검증한다
row 폭을 바꾸면 parquet 고정 오버헤드 비중이 달라져 총 바이트가 최대 12.8%p 어긋났다.
`gen/make_skewed.py` 는 이제 샘플을 실제로 써보고 bytes/row 를 재서 행 수를 역산한다.

### 6. Spark 4.0 은 OOM 을 `FAILED_READ_FILE` 로 표시한다
`FileDataSourceV2$.attachFilePath` 의 `MatchError` 때문이다. parquet 파일이 멀쩡한데
"파일 읽기 실패"가 뜨면 **힙 부족을 의심**할 것. `Caused by` 체인을 끝까지 봐야 보인다.

## 알려진 이슈 / 주의

- **Spark 진행바가 캐리지리턴으로 출력을 덮는다** → `spark.ui.showConsoleProgress=false`로 끔. 로그를 grep할 때 `tr '\r' '\n'`이 필요할 수 있다.
- eventlog는 **평문 단일 파일**로 설정 (`eventLog.compress=false`, `rolling.enabled=false`). zstd 롤링이면 파서에 추가 의존성이 필요해진다.
- 차트 텍스트는 **영어**. 리눅스 한글 폰트 누락으로 글자가 깨지는 문제를 원천 차단하고, LinkedIn 대상에도 맞다. 문서·주석은 한글.
- `spark.local.dir will be overridden` 경고는 local 모드에서 무해하다.
