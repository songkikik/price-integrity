# 실시간 시세 정합성 모니터링 (price-integrity)

여러 소스에서 동일 자산의 시세를 동시에 수집하고, **소스 간 정합성(cross-check)** 과 **이상치(anomaly)** 를 실시간으로 모니터링하는 데이터 파이프라인.

> 사이드 프로젝트. Kubernetes(k3s) 위에 Airflow + dbt + Postgres + Grafana 를 자체 운영하며 "한 클러스터에서 멀티 도메인 데이터 플랫폼을 운영하는" 경험을 쌓는 것이 목표.

---

## 왜 만드는가

- **시세는 같은 자산이라도 소스마다 다르다.** 거래소·환율 제공자 사이의 스프레드, 적재 지연, 값 고정(stale) 같은 미세한 불일치는 KPI·알림·자동매매 시스템의 신뢰를 흔든다.
- **데이터 정합성(integrity)** 은 분석가가 가져갈 수 있는 가장 강한 무기 중 하나다. "수치는 정확한가, 같은 수치인가, 늦지 않았는가" 를 시스템으로 검증한다.
- 부가 목표: 사내 인프라가 아닌 **본인 홈랩 (가정 PC + k3s)** 에서 24/7 운영하며 K8s · Airflow · dbt · GitOps · 옵저버빌리티를 한 번에 실습.

---

## 아키텍처

```
┌────────────────────────────┐    Tailscale VPN     ┌─────────────────────────────────────┐
│  MacBook Air (개발)        │ ◀─────────────────▶  │  가정 PC = 24/7 K8s 노드            │
│  - kubectl / Lens          │                       │  Ubuntu 22.04 LTS + k3s             │
│  - VSCode + Remote-SSH     │                       │                                      │
│  - dbt CLI                 │                       │  ┌──────────────────────────────┐  │
│  - git push                │                       │  │ Airflow (KubernetesExecutor) │  │
└────────────────────────────┘                       │  │   ├─ DAG: 1min crypto KR     │  │
            │                                        │  │   ├─ DAG: 1min crypto Global │  │
            └───── git push ────▶  GitHub  ─────────▶│  │   ├─ DAG: 1min FX            │  │
                                                     │  │   └─ DAG: 1min Equity        │  │
                                                     │  ├──────────────────────────────┤  │
                                                     │  │ Postgres 16 (raw / stg / mart)│ │
                                                     │  ├──────────────────────────────┤  │
                                                     │  │ dbt-postgres (변환)          │  │
                                                     │  ├──────────────────────────────┤  │
                                                     │  │ Grafana + Prometheus (모니터)│  │
                                                     │  └──────────────────────────────┘  │
                                                     │            │                         │
                                                     │            ▼                         │
                                                     │   Discord Webhook (알림)            │
                                                     └─────────────────────────────────────┘
```

---

## 자산 × 소스 매트릭스

수집 주기는 **1분 (모든 소스 rate limit 내 검증 완료)** .

| 자산 | 1차 소스 | 2차 소스 | 비고 |
|---|---|---|---|
| USD/KRW | Yahoo Finance (`KRW=X`) | 한국은행 ECOS | 둘 다 공식 무료 API |
| BTC/KRW | Upbit | Bithumb | 한국 시장 1·2위 |
| BTC/USDT | Binance | Coinbase | 글로벌 1·2위 |
| ETH/KRW | Upbit | Bithumb | 한국 시장 1·2위 |
| KOSPI (`^KS11`) | Yahoo Finance | Naver Finance | Naver 는 비공식 스크래핑 (UA·jitter 필요) |
| S&P 500 (`^GSPC`) | Yahoo Finance | Stooq | Stooq 는 CSV 방식 |

→ 분당 총 12 요청. 모든 소스 무료 한도 내.

---

## 적재 구조

로컬 Postgres on k3s. 비용 0.

```
Postgres 16
├─ db: airflow_meta             # Airflow 메타DB
└─ db: price_warehouse
    ├─ schema: raw              # 소스별 1:1 원본 적재
    │   ├─ raw_upbit_ticker
    │   ├─ raw_bithumb_ticker
    │   ├─ raw_binance_ticker
    │   ├─ raw_coinbase_ticker
    │   ├─ raw_yahoo_price
    │   ├─ raw_ecos_fx
    │   ├─ raw_naver_price
    │   └─ raw_stooq_price
    ├─ schema: staging          # dbt stg_*: 통일 스키마로 정제
    │   └─ stg_price_unified(asset, source, ts_utc, price, volume, ...)
    └─ schema: mart             # dbt fct_/dim_
        ├─ fct_price_spread_1m
        ├─ fct_price_anomaly_1m
        └─ dim_source · dim_asset
```

---

## 이상치 알림 룰 (초안)

| 범주 | 룰 | 임계값 (초안) | 심각도 |
|---|---|---|---|
| **A. Cross-check spread** | 같은 자산 두 소스 가격 차 / 평균 | 외환 >0.3% / 한국암호 >1.0% / 글로벌암호 >0.5% / 주가 >0.5% | WARN |
| **A'. 시간 정합성** | 두 소스 timestamp 차이 | > 5분 | WARN |
| **B. Freshness** | 마지막 적재 시각 경과 | > 3분 (1분 주기 + 2분 buffer) | INFO → CRIT (>10분) |
| **C. Z-score 이상** | 최근 60분 평균·표준편차 대비 \|z\| | > 3 | CRIT |
| **C'. 분당 변동률** | 1분 % 변동 | 외환 >0.5% / 암호 >3% / 주가 >1% | WARN |
| **D. Stale value** | 동일 가격 연속 분 수 | 외환 30분 / 암호 5분 / 주가 10분 (장중) | WARN |

→ 알림 채널: **Discord Webhook** (셋업 30초). 라우팅: `INFO/WARN` → `#alerts-low`, `CRIT` → `#alerts-high`.
→ 임계값은 **2주차에 실데이터 분포 보고 재조정** 예정.

---

## 폴더 구조

```
price-integrity/
├── README.md                # 이 파일
├── .env.example             # 환경 변수 템플릿
├── .gitignore
├── collectors/              # 소스별 수집기 (Python)
├── airflow/
│   └── dags/                # 1분 주기 수집 DAG
├── dbt/
│   └── models/              # staging / intermediate / marts
├── alerts/                  # 이상치 룰 정의 + Discord notifier
├── k8s/
│   └── helm-values/         # Airflow / Postgres / Grafana helm values
├── docs/                    # 아키텍처·소스·알림 룰 상세 문서
└── scripts/                 # k3s 셋업·백필 스크립트
```

---

## 로드맵 (4주 가정)

| 주차 | 목표 |
|---|---|
| **1주차** | 가정 PC Ubuntu + k3s 셋업, Postgres·Airflow helm 설치, Upbit 1개 소스 raw 적재 DAG 동작 확인 |
| **2주차** | 나머지 소스 7종 collector + DAG, dbt staging/mart 모델, 임계값 실데이터 분포 보고 재조정 |
| **3주차** | Grafana 대시보드, Discord 알림 라우팅, freshness 모니터링 |
| **4주차** | 회고, 상세 docs, 블로그 글 1편, 면접 시연 시나리오 정리 |

---

## 기술 스택

- **인프라**: Ubuntu 22.04 LTS · k3s · Tailscale (원격 접근)
- **오케스트레이션**: Apache Airflow (KubernetesExecutor, helm chart)
- **저장**: PostgreSQL 16 (raw / stg / mart 3-schema)
- **변환**: dbt-postgres
- **모니터링**: Prometheus + Grafana
- **알림**: Discord Webhook
- **언어**: Python 3.12 (uv 로 의존성 관리)

---

## 라이선스 / 면책

- 라이선스: MIT (예정)
- 본 프로젝트의 수집·분석은 학습 목적이며 투자 자문이 아님.
- 외부 API 사용은 각 제공자의 ToS·Rate limit 을 준수.

---

## 진행 상황

- [x] 레포 생성 + 폴더 구조 + README 초안 (2026-05-22)
- [ ] 가정 PC Ubuntu 설치 + k3s 셋업
- [ ] Postgres / Airflow helm 설치
- [ ] 첫 collector (Upbit) + DAG 동작 확인