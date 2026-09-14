# 03. 실험 매트릭스 — "모든 경우의 수"를 어떻게 처리할 것인가

## 왜 full factorial은 불가능한가

로컬에서 가능한 factor를 전부 전개하면:

| Factor | 레벨 | 수 |
|---|---|---:|
| skew degree | 1·2·4·8·16·32·64 | 7 |
| row width | 64B·256B·1KB | 3 |
| cores/executor | 1·2·3·6 | 4 |
| executor memory | 1g·1.5g·3g | 3 |
| shuffle partitions | 200·1000·4000 | 3 |
| AQE | off·on | 2 |
| compression codec | lz4·zstd·snappy·none | 4 |
| free RAM (cgroup) | 16g·8g·4g | 3 |
| `vm.dirty_ratio` | 5·20·60 | 3 |
| workload | sort·agg·join | 3 |

`7×3×4×3×3×2×4×3×3×3 = 163,296` 조건 × 5회 반복 = **816,480 run**
run당 2분이면 **약 1,134일 ≈ 3년.**

**"모든 경우의 수"는 불가능하다. 하지만 "모든 RQ"는 3주면 된다.**

---

## 대안: 단계형 설계

교차를 전부 도는 게 아니라, **"이 두 변수가 상호작용한다"는 가설이 있는 쌍만** 교차시킨다. 나머지는 baseline 고정 (OFAT).

| Stage | 설계 | run | 답하는 것 | 상태 |
|---|---|---:|---|---|
| **S1** | skew sweep @ baseline → **cliff 위치 확정** | 35 | RQ1 / **P1** | 🔵 실행중 |
| **S2** | skew × row width (64·256·1024B) | 105 | **RQ5 / P2·P3** | |
| **S3** | skew × cores-per-executor (1·2·3·6, 메모리 비례) | 140 | **RQ2 / P4** | |
| **S4** | skew × partitions(200·1000·4000) × AQE(off·on) | 210 | **RQ6 / P6** + RQ7 | |
| **S5** | skew × free RAM (cgroup 16g·8g·4g) | 105 | **RQ4-a / P5** | |
| **S6** | skew × `vm.dirty_ratio` (5·20·60) | 105 | **RQ4-b** | |
| **S7** | codec × storage, **cliff 양옆 2점에서만** | 40 | **RQ3** | |
| **S8** | 핵심 교차만 join 워크로드로 재현 | 150 | 일반화 검증 | |
| **S9** | EC2: PMU + 실제 NVMe, **cliff 양옆에서만** | 40 | 하드웨어 레이어 | |
| | **합계** | **≈ 930** | **RQ1–RQ7 전부** | |

run당 ~2.5분 → **순수 실행 약 39시간.** 자동화해서 밤에 돌리면 **2~3주.** 분석·글쓰기 포함 8주에 맞는다.

### 핵심 원칙

> **S1에서 cliff을 먼저 찾고, 이후 모든 stage는 "cliff이 어디로 이동하는가"만 본다.**

그러면 각 stage는 7~12개 점만 필요하고 전체 격자를 돌 이유가 사라진다. 종속변수가 "latency"가 아니라 **"cliff의 위치"**가 되는 것이 이 설계의 전부다.

---

## Baseline 고정값

S1에서 확정. 이후 모든 stage는 여기서 **한 번에 한 축만** 벗어난다.

```
total data      8 GiB          (byte 통제, 전 실험 불변)
row width       256 B
partitions      200            (bypassMergeSortThreshold 경계와 동일 — S4에서만 변경)
cores           4
executor mem    1500m          (pool 720MB → cliff 예상 S≈13)
AQE             off            (S4에서만 on)
codec           lz4
workload        sort
reps            5
```

---

## 반복·순서 통제

- **rep을 바깥 루프에 둔다.** skew별로 몰아 돌리면 시간 순서 효과(디스크 상태, 열 누적, 백그라운드 프로세스)가 skew와 교락된다. rep마다 전체 skew를 한 바퀴 돈다. (`s1_sweep.sh`에 구현됨)
- run마다 `drop_caches`.
- **median + IQR로 보고한다.** 평균 금지.
- **cliff 주장 조건**: 구간 median 변화량 > 그 구간의 IQR. `plot_cliff.py`가 기울기와 IQR을 같이 출력한다.

---

## Stage별 판정 기준

| Stage | 참이면 | 거짓이면 |
|---|---|---|
| S1 | 계획대로 8주 진행 | 완만한 무릎 → RQ4 축소, RQ2/5/6 재편 / 노이즈에 묻힘 → B6부터 다시 |
| S2 | **"AQE는 byte만 봐서 record-skew를 놓친다"** → 블로그 #1 + JIRA 후보 | "bytes가 지배" — 이것도 통념 검증 결과로 씀 |
| S3 | **"cores-per-executor가 skew 내성을 결정"** → LinkedIn #1 | "core 수 무관" — 통념 반박 실패, 짧게 기록 |
| S4 | **"AQE가 보는 크기가 실측과 다르다"** → JIRA | 차이 무시 가능 — 항목 폐기 |
| S5/S6 | **"cliff 위치를 커널이 정한다"** → 핵심 결과 | Spark/JVM에서 설명 종료 (깊이의 원칙상 정당한 결론) |
| S7 | "disk가 아니라 CPU" — Ousterhout 재확인 | disk-bound — 그 자체가 반전 |

---

## 디스크 예산

ext4.vhdx가 C:(212GB free)에 있으므로 **데이터셋 총량 100GB 이하** 유지.

| Stage | 데이터셋 | 용량 |
|---|---|---|
| S1 | 8GiB × 7 skew | 56 GB |
| S2 | 8GiB × 7 × 2 추가 row width | +112 GB ⚠️ |

> S2는 예산 초과. **row width별로 순차 생성 → 실행 → 삭제**하는 방식으로 돌려야 한다. `s2_sweep.sh` 작성 시 반영할 것.
