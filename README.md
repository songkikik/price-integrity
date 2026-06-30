# price-integrity

여러 거래소에서 동일 자산의 시세를 1분 단위로 수집하고 **소스 간 정합성(cross-check)** 과 **이상치(anomaly)** 를 모니터링하는 데이터 파이프라인 사이드 프로젝트.

---

## 왜 만드는가

- **시세는 같은 자산이라도 소스마다 다르다.** 거래소 간 스프레드, 적재 지연, 값 고정(stale) 같은 불일치는 분석 지표의 신뢰를 흔든다.
- **데이터 정합성(integrity)** 을 시스템으로 검증하는 것이 이 프로젝트의 핵심.
- 부가 목표: 홈랩 (가정 PC + k3s) 위에서 Airflow · ClickHouse · dbt · Grafana를 24/7 운영하며 실습.

---

## 아키텍처

```
┌────────────────────────┐    Tailscale     ┌────────────────────────────────────┐
│  MacBook Air (개발)    │ ◀──────────────▶ │  가정 PC (24/7 K8s 노드)           │
│  - kubectl / Lens      │                  │  Ubuntu 22.04 LTS + k3s            │
│  - VSCode + Remote-SSH │                  │                                    │
│  - dbt CLI             │                  │  ┌────────────────────────────┐    │
│  - git push            │                  │  │ Airflow (1분 수집 DAG)     │    │
└────────────────────────┘                  │  ├────────────────────────────┤    │
            │                               │  │ ClickHouse (MergeTree)     │    │
            └── git push ──▶ GitHub ───────▶│  ├────────────────────────────┤    │
                                            │  │ dbt-clickhouse (변환)      │    │
                                            │  ├────────────────────────────┤    │
                                            │  │ Grafana + Prometheus       │    │
                                            │  └────────────┬───────────────┘    │
                                            └───────────────┼────────────────────┘
                                                            │ Slack Webhook
                                                            ▼ [price-integrity][severity]
```

> **v0.1 로컬 개발**: k3s 없이 Docker Compose로 ClickHouse 1개만 띄워서 시작. k3s는 마일스톤 1 달성 후 전환.

---

## 자산 × 소스 매트릭스

> v0.1 (PoC): **BTCKRW × Upbit/Bithumb** 만 수집. 나머지는 v0.2+ 단계적 확장.

| 자산 | 소스 A | 소스 B | quote_ccy | 단계 |
|---|---|---|---|---|
| `BTCKRW` | Upbit `/v1/ticker?markets=KRW-BTC` | Bithumb `/public/ticker/BTC_KRW` | KRW | **v0.1 PoC** |
| `BTCUSDT` | Binance `/api/v3/ticker/price` | Bybit `/v5/market/tickers` | USDT | v0.2 |
| `ETHKRW` | Upbit | Bithumb | KRW | v0.2 |
| `USDKRW` | (fx 도메인, TBD) | | KRW | v0.2 |
| `KOSPI` | (equity 도메인, TBD) | | KRW | v0.3 |
| `SP500` | (equity 도메인, TBD) | | USD | v0.3 |

> 수집 주기: 1분 (모든 소스 rate limit 검증 완료). 자세한 API 엔드포인트·스키마는 [docs/source_matrix.md](docs/source_matrix.md) 참조.

---

## 데이터 레이어

```
raw   → Python collector가 API 응답 JSON 1:1 적재 (컬럼 정제 X)
stg   → 공통 스키마 정규화 (ts_utc, price, source_code 통일)
int   → 자산×분 grain long 형태 집계 (소스 UNION)
mart  → spread 계산 · anomaly 감지 · 알림 발사
```

자세한 테이블 정의·ERD는 [docs/source_matrix.md](docs/source_matrix.md) 참조.

---

## 이상치 감지 룰 (PoC 초안)

| rule_code | 정의 | 임계값 | severity |
|---|---|---|---|
| `spread_over_threshold` | 두 소스 간 `spread_pct > X` N분 연속 | X=0.5%, N=2 | warn |
| `spread_critical` | 두 소스 간 `spread_pct > Y` 즉시 | Y=2.0% | critical |
| `source_stale` | 특정 소스 `tick_count = 0` N분 연속 | N=5 | warn |
| `all_sources_stale` | 자산의 모든 소스 stale | N=2 | critical |
| `price_jump_single_source` | 단일 소스 1분 내 `\|Δprice\|/price > Z` | Z=1.0% | info |

> 임계값은 2주차에 실데이터 분포 보고 재조정.

---

## 알림 채널

**Slack 개인 워크스페이스 webhook** (`gihun-sideprojects` 워크스페이스, `#alerts-price-integrity` 채널).

| 항목 | 값 |
|---|---|
| 메시지 prefix | `[price-integrity][{severity}]` |
| 환경변수 | `SLACK_WEBHOOK_URL` |
| cooldown (warn) | 10분 |
| cooldown (critical) | 5분 (severity 상승 시 즉시) |

---

## 폴더 구조

```
price-integrity/
├── README.md
├── .env.example             # 환경변수 템플릿 (커밋 O, 값 X)
├── .gitignore
├── pyproject.toml           # Python 의존성 (uv)
├── collectors/              # 소스별 시세 수집기 (Python)
│   └── samples/             # API 응답 JSON 샘플 (raw 스키마 확정용)
├── alerts/                  # Slack webhook notifier
├── airflow/
│   └── dags/                # 1분 주기 수집 DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example # 커밋 O (값 없음). 실제 profiles.yml은 .gitignore
│   ├── seeds/               # dim_asset.csv, dim_source.csv
│   └── models/
│       ├── staging/         # stg_upbit__price_tick 등
│       ├── intermediate/    # int_crypto__price_tick_long 등
│       └── marts/           # fct_crypto__price_spread 등
├── k8s/
│   └── helm-values/         # Airflow / Grafana / ClickHouse Helm values
├── docs/
│   └── source_matrix.md     # v0.1 데이터 모델 설계서 (ERD 포함)
└── scripts/                 # k3s 셋업 · 백필 스크립트
```

---

## 기술 스택

| 영역 | 기술 |
|---|---|
| 인프라 | Ubuntu 22.04 LTS · k3s · Tailscale |
| 오케스트레이션 | Apache Airflow (KubernetesExecutor) |
| 저장 | **ClickHouse** (MergeTree — v0.1은 Docker Compose 로컬, k3s는 마일스톤 1 이후) |
| 변환 | **dbt-clickhouse** |
| 모니터링 | Prometheus · Grafana |
| 알림 | **Slack Webhook** (개인 워크스페이스 `gihun-sideprojects`) |
| 언어 | Python 3.12 (uv) |

---

## 로드맵

| 단계 | 내용 | 상태 |
|---|---|---|
| 모델링 | `docs/source_matrix.md` v0.1 확정 | ✅ |
| seed | `dim_asset` / `dim_source` CSV + dbt 프로젝트 초기화 | 🔄 |
| API 샘플 | Upbit · Bithumb JSON 응답 캡처 → raw 스키마 확정 | ⬜ |
| 인프라 | Docker Compose ClickHouse 로컬 실행 | ⬜ |
| collector | Upbit 1분 수집기 (Python + ClickHouse insert) | ⬜ |
| dbt stg | `stg_upbit__price_tick` 첫 모델 | ⬜ |
| collector 2 | Bithumb 추가 → `int_crypto__price_tick_long` | ⬜ |
| mart | `fct_crypto__price_spread` + 룰 1개 | ⬜ |
| 알림 | Slack webhook + `fct_crypto__anomaly_event` → dispatch | ⬜ |
| **마일스톤 1** | BTCKRW Upbit↔Bithumb spread 모니터링 E2E 동작 | ⬜ |

> 마일스톤 1 달성 후 블로그 1편 작성 예정.

---

## 라이선스 / 면책

MIT (예정). 본 프로젝트는 학습 목적이며 투자 자문이 아님. 외부 API 사용은 각 제공자의 ToS를 준수.
