# 21. S8b — 계단은 `sort` 밖에서도 서는가, 집계는 왜 못 재는가

실행: 2026-09-19 · EC2 m6id.4xlarge (spot) · **32 run** (성공 26 / 실패 6)
스크립트: `runner/s8b_sweep.sh` · 그림: `results/s8b/figures/per_key_ops.png`
선행: `19-s8-generalization.md` 의 미해결 구멍 ①

---

## 0. 요약

| | |
|---|---|
| **계단은 일반화된다** | ✅ window 의 `peak_exec_mem` 이 sort 와 **모든 skew 에서 소수점까지 동일** |
| **spill 양은 연산자마다 다르다** | window 가 skew 32 에서 1,529 MiB, sort 는 689 MiB (2.2배) |
| **cliff 위치도 다르다** | window 는 skew 16, sort 는 24 에서 데이터 비례 구간 진입 |
| **집계는 여전히 못 잰다** | ⛔ 단, **왜 못 재는지는 확정했다.** 결과가 아니라 한계다 |
| 실무 한 줄 | 같은 입력에서 sort/window 는 **1,054 MiB** 를 처리하고, `collect_list` 는 **293 MiB** 에서 멈춘다 |

## 1. 왜 했나

S8 은 집계 팔을 `collect_list(substring(payload, 1, 8))` 로 잡았다. 256B 행을 8B 로
줄여 모으니 shuffle 이 32 배 작아졌고, hot 파티션이 1,054 MiB 가 아니라 **52 MiB** 였다.
실행 풀(720 MiB) 근처도 못 갔다.

"집계는 계단이 없다"가 아니라 **시험이 물렀다.** 그걸 고치러 왔다.

## 2. 먼저 알아낸 것 — 질문 자체가 잘못 놓여 있었다

payload 를 안 자른 `agg_wide`(= `collect_list(payload)`) 로 스모크를 돌렸더니
둘 다 실패했다. 그런데 **실패 방식이 서로 달랐고**, 그게 설계를 바꿨다.

```
base (폴백 임계 128 기본값) : stage 1(map-side 부분집계)에서 OOM. reduce 근처도 못 감
fb0  (즉시 폴백)            : stage 1·2 전부 통과, 658 MiB spill 까지 하고 그 뒤 JVM 사망
```

이유는 단순하다. hot key 의 **출력 한 행**이 4.6M × 248B = **1.15 GB** 다.
풀(720 MiB)을 넘기려면 출력이 반드시 힙보다 커진다.

> **`collect_list` 로는 "풀을 넘었다"와 "출력이 너무 크다"를 원리적으로 분리할 수 없다.**
> 힙을 키우면 풀도 같이 커져서 넘지를 못하고, 넘게 만들면 출력이 힙을 넘는다.
> 이 워크로드로는 계단을 잴 수 없다. 설정을 바꿔도 안 된다.

그래서 **키별 연산이되 출력에 경계가 있는 것**으로 갈아탔다.

## 3. 설계

| 팔 | 내용 | run |
|---|---|---:|
| `window` | `row_number() over (partition by key order by payload)` | 12 |
| `sort` | 기준선. **같은 세션·같은 인스턴스**에서 나란히 | 12 |
| `agg_wide` base / fb0 | `collect_list(payload)`. **실패를 특성화한다** | 8 |

skew 8·16·24·32 · cores 4 · exec-mem 1500m (풀 720 MiB) · partitions 200 · AQE off.
window/sort 는 3 rep, agg_wide 는 1 rep.

`window` 를 고른 이유: 현장에서 skew 로 제일 자주 터지는 **키별 중복제거 / top-N**
패턴이다. hot 파티션이 축약 없이 reduce 로 오고(계단 조건 충족), 출력은 입력 행마다
한 행이라 경계가 있다. `WindowExec` 는 `UnsafeExternalSorter` 를 쓴다.

## 4. 결과 — 계단은 sort 고유가 아니다

hot 파티션이 읽은 바이트는 두 팔이 **완전히 동일**하다 (291.5 / 564.6 / 818.0 / 1053.9 MiB).

| | skew 8 | 16 | 24 | 32 |
|---|---:|---:|---:|---:|
| **peak exec mem (MiB)** | | | | |
| window | 200.0 | 560.0 | **712.0** | **712.0** |
| sort | 200.0 | 560.0 | **712.0** | **712.0** |
| **spill (MiB)** | | | | |
| window | 136.5 | 463.8 | 1293.0 | 1529.3 |
| sort | 136.5 | 136.5 | 687.2 | 689.4 |
| **wall (s)** | | | | |
| window | 33.5 | 37.3 | 42.4 | 45.4 |
| sort | 31.0 | 33.1 | 36.9 | 38.8 |

### 천장이 같다 — 그것도 소수점까지

포화 구간(skew 24·32)에서 두 팔 모두 **712.0 MiB 단 하나의 값**만 나온다.
6 run 씩, 총 12 run 이 전부 같다.

> S1 에서 "하드웨어가 달라도 712 MiB" 였고, S8 에서 "join 은 616.1 MiB" 였다.
> 이제 **연산자가 달라도 UnsafeExternalSorter 를 쓰면 712.0** 이라는 것까지 확인됐다.
> 천장은 워크로드가 아니라 **어떤 메모리 소비자를 쓰느냐**가 정한다.

### 그런데 spill 은 2.2 배 다르고 cliff 도 더 이르다

sort 는 skew 16 까지 평탄구간(136.5 MiB)인데 window 는 skew 16 에서 이미
463.8 MiB 로 올라간다. cliff 가 **window 16 / sort 24** 로 갈린다.

`row_number` 는 정렬한 결과를 다시 읽으며 순번을 매긴다. `sortWithinPartitions` 가
한 번 뱉고 끝나는 것과 달리 왕복이 있다. 이 읽기가 가장 정합적이지만
**확인하지 않았다** — `WindowExec` 의 sorter 사용을 직접 계측해야 한다.

## 5. 집계는 왜 못 재는가 — 결과가 아니라 한계다

| 팔 | skew | hot read | 결과 | 죽은 지점 |
|---|---:|---:|---|---|
| base | 8 | 292.8 MiB | **ok** (spill 0) | — |
| base | 16 | — | OOM | stage 1, 75/81 태스크 |
| base | 24 | — | OOM | stage 1, 74/81 |
| base | 32 | — | OOM | stage 1, 69/81 |
| fb0 | 8 | 292.8 MiB | **ok** (spill 0, 폴백함) | — |
| fb0 | 16·24·32 | — | OOM | stage 1, 65~72/81 |

**6개 실패가 전부 stage 1(map-side 부분집계)에서 났다.** reduce 스테이지에
도달조차 못 했다.

> ⚠️ **그래서 실패 run 의 `spill = 0` 은 증거로 쓸 수 없다.**
> `spill_disk_total` 은 reduce 태스크에서 집계하는데 reduce 태스크가 아예 없었다.
> "spill 안 했다"가 아니라 **"잴 게 없었다"** 이다. 그림 패널 D 를 spill 막대가
> 아니라 "끝까지 처리한 가장 큰 hot 파티션"으로 바꾼 이유다.

유효한 데이터는 **skew 8 한 점**이다. 거기서는 이렇다:

```
같은 292.8 MiB hot 파티션을 놓고
  sort / window : spill 136.5 MiB 하고 처리
  collect_list  : spill 0,  메모리에 그대로 들고 처리
```

집계가 안전밸브를 안 썼다는 **한 점짜리 관측**이다. 주장으로 쓰기엔 n=1 이다.

### 구조적으로는 이렇게 정리된다 (측정 아님)

- **축약 가능한 집계**(`count`, `sum`) — map-side combine 이 hot 파티션을 reduce 에
  도달하기 전에 없앤다. 계단이 생길 수가 없다. S8 의 `count` 가 전 구간 평평했던 이유다.
- **축약 불가능한 집계**(`collect_list` of raw rows) — hot key 의 출력 한 행이
  힙보다 커진다. 계단에 닿기 전에 죽는다.
- **그 사이가 없다.** 그래서 이 하네스로 "집계의 계단"을 재는 것은 불가능하다.

## 6. 판정

| | |
|---|---|
| 계단 메커니즘이 sort 밖에서 서는가 | ✅ **참.** window 가 천장·위치 모두 재현 (12 run, 712.0 MiB 단일값) |
| 천장은 연산자가 정하는가 | ✅ 같은 sorter 면 같은 천장. S8 의 join(616.1)과 합쳐 일관 |
| spill 양은 일반화되는가 | ❌ **아니다.** window 1,529 vs sort 689 MiB (skew 32) |
| 집계의 계단 | ⛔ **측정 불가 — 이유 확정.** 축약되거나 출력이 터지거나 둘 중 하나다 |
| 신뢰도 | 높음 (rep 퍼짐 window 0.75s / sort 0.39s, 포화 천장 12 run 전부 동일) |

## 7. 실무로 옮기면

- **`row_number` 같은 키별 윈도우는 정렬과 똑같이 위험하다.** 같은 skew 에서
  같은 천장에 닿고, spill 은 오히려 **2.2 배 더** 한다. "집계니까 괜찮겠지"가 아니다.
- **`collect_list` / `collect_set` 을 skew 키에 쓰지 마라.** spill 로 버티는 길이
  없다. 292.8 MiB 는 되고 564.6 MiB 는 안 됐다. 그 사이 어딘가에서 죽는다.
- **`count`·`sum` 류는 실제로 안전하다** (S8 에서 전 구간 평평). 안전한 이유는
  "집계라서"가 아니라 **map-side 에서 축약돼서** 다. 축약이 안 되는 집계는 안전하지 않다.

## 8. 남은 구멍

1. window 의 cliff 가 왜 더 이른지는 **추론**이다. sorter 왕복을 계측하지 않았다.
2. 집계 계단은 이 하네스로 닫을 수 없다. 재려면 **출력이 작으면서 축약도 안 되는**
   집계가 필요한데, 그런 게 있는지부터 모르겠다.
3. `agg_wide` 의 map-side OOM 은 입력 split 크기에 비례한다. `maxPartitionBytes` 를
   16 MiB 로 줄이면 stage 2 까지는 간다 (스모크에서 확인). 본 스윕에는 안 넣었다 —
   팔마다 설정이 달라지면 비교가 깨진다.
4. 여전히 단일 노드다.

## 9. 재현

```bash
bash runner/s8b_sweep.sh                 # 32 run, m6id.4xlarge 기준 약 25분
python parse/parse_eventlog.py --tag s8b
python analysis/plot_s8b.py --tag s8b
```

> 이 스윕은 한 번 결과를 통째로 잃고 다시 돌린 것이다. 원인은 대기 스크립트의
> `pgrep -f` 자기매칭 버그였다 (커밋 `bba2377`). `env/ec2-sync.sh wait` 가
> 완료 표식 파일을 보고 5분마다 중간 회수하도록 고쳤다.
