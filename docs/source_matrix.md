# Source Matrix & Data Model (v0.1)

> price-integrity 사이드 프로젝트의 소스·계층·grain 정의서.
> v0.1은 PoC(BTCKRW × Upbit/Bithumb) 범위까지만 확정, FX/Equity는 자리만 잡음.

---

## 1. 프로젝트 목적

여러 거래소·정보제공처에서 동일 자산의 시세를 1분 단위로 수집하고
- **소스 간 정합성(cross-check)**: 같은 자산의 가격 차이(spread)가 임계치를 넘는지
- **소스별 이상치(anomaly)**: 단일 소스에서 비정상적 가격/거래량 점프가 있는지
- **소스 신선도(freshness)**: 소스가 stale(중단) 상태인지

위 세 가지를 모니터링하고 Slack webhook으로 알린다.

---

## 2. 자산 도메인 분리

운영 시간·통화·시장 특성이 달라 **한 모델에 묶지 않고 도메인별로 fact 테이블을 분리**한다.

| 도메인 | 자산 | 운영 시간(KST) | PoC 단계 |
|---|---|---|---|
| **crypto** | BTCKRW, BTCUSDT, ETHKRW | 24h × 7 | **1단계 (현재)** |
| **fx** | USDKRW | 평일 24h | 2단계 |
| **equity** | KOSPI, S&P500 | 장 시간만 (지역별 상이) | 3단계 |

→ v0.1은 **crypto 도메인만** 모델링. fx/equity는 자리만 잡고 v0.2+에서 채움.

---

## 3. 소스 매트릭스 — crypto 도메인

| asset_code | source A | source B | quote_ccy | rate limit(공개치) | 인증 |
|---|---|---|---|---|---|
| `BTCKRW` | `upbit` `/v1/ticker?markets=KRW-BTC` | `bithumb` `/public/ticker/BTC_KRW` | KRW | Upbit 600/min, Bithumb 135/sec | 둘 다 unauth OK |
| `BTCUSDT` | `binance` `/api/v3/ticker/price?symbol=BTCUSDT` | `bybit` `/v5/market/tickers?category=spot` | USDT | Binance 6000 weight/min, Bybit 120/sec | unauth OK |
| `ETHKRW` | `upbit` | `bithumb` | KRW | (위와 동일) | unauth OK |

> **확인 필요**: rate limit·응답 스키마는 실제 호출로 dry-run 검증 후 stg 모델 컬럼 매핑 확정.

---

## 4. 데이터 계층 분담

```
raw   → 1:1 API 응답 보관 (JSON only, 컬럼 정제 X)
stg   → 1:1 정제 (타입, ts_utc 정규화, dedup)
int   → 도메인 통합 (자산×분 grain의 long 형태)
mart  → 분석·알림용 (spread 계산, anomaly event)
```

### 4.1 raw 레이어

테이블별 **소스 단위로 1:1**. API 호출 1회당 1 row, 응답 통째 JSON으로 보관.

| 테이블 | grain | 컬럼 |
|---|---|---|
| `raw_upbit__ticker` | API call 1회 | `ingested_at TIMESTAMP, raw_payload JSON` |
| `raw_bithumb__ticker` | API call 1회 | `ingested_at TIMESTAMP, raw_payload JSON` |

**Why raw 분리:** API 스키마 변경 시 소급 재처리(`stg` 재빌드) 가능. 1분 단위 호출이라 저장량 부담 적음(JSON 행 1~2KB 추정).

### 4.2 stg 레이어 — 공통 스키마

소스가 달라도 **stg 출력 컬럼은 동일**. 이게 cross-check의 전제.

| 컬럼 | 타입 | 의미 | 비고 |
|---|---|---|---|
| `asset_code` | STRING | `'BTCKRW'` 등 매트릭스 ID | dim_asset FK |
| `source_code` | STRING | `'upbit'` 등 | dim_source FK |
| `ts_utc` | TIMESTAMP | API 응답의 거래시각(마이크로초) | 적재시각 아님 |
| `ts_minute_utc` | TIMESTAMP | `ts_utc`를 1분 floor | int 레이어 조인 키 |
| `price` | NUMERIC(28,8) | quote_ccy 단위 그대로 (환산 X) | |
| `quote_ccy` | STRING | `'KRW'`, `'USDT'`, `'USD'` | |
| `volume_24h` | NUMERIC | 거래소 제공 시 (nullable) | |
| `raw_payload_id` | STRING | raw 테이블 FK (PK는 raw의 `ingested_at`) | trace용 |
| `ingested_at` | TIMESTAMP | dbt 적재 시각 | |

**PK**: `(asset_code, source_code, ts_utc)` — 동일 ts_utc 중복 시 dedup(최신 ingested_at 우선).

**모델 명명:**
- `stg_upbit__price_tick`
- `stg_bithumb__price_tick`
- (v0.2) `stg_binance__price_tick`, `stg_bybit__price_tick`, …

### 4.3 int 레이어

**도메인별 1개**. crypto 도메인은:

#### `int_crypto__price_tick_long`
- **Grain**: `(asset_code, source_code, ts_minute_utc)` — 1분 1소스 1자산당 1 row
- 1분 내 여러 tick이 있으면 **분의 마지막 tick(close)** 을 채택 → [D1 확정](#decisions)
- `price` 컬럼 의미 = "해당 분의 마지막 체결가 스냅샷"
- v0.2 WebSocket 전환 시에는 OHLCV 4종(`open/high/low/close/volume_minute`) 컬럼 확장하고 `close`를 동일 역할로 둘 것
- stg_upbit + stg_bithumb UNION

| 컬럼 | 의미 |
|---|---|
| `asset_code, source_code, ts_minute_utc` | PK |
| `price` | 1분의 대표가 |
| `tick_count` | 해당 분에 들어온 raw tick 수 (freshness 지표) |
| `quote_ccy` | |

### 4.4 mart 레이어

#### `fct_crypto__price_minute_long`
- int를 그대로 노출 (rename + 인덱스/파티션 최적화만). cross-check의 원천.
- **Grain**: `(asset_code, source_code, ts_minute_utc)`

#### `vw_crypto__price_minute_wide` (view, pivot)
- BI/대시보드용 wide view. 소스 늘어도 view만 수정.
- **Grain**: `(asset_code, ts_minute_utc)`
- 컬럼: `price_upbit, price_bithumb, price_binance, ...`

#### `fct_crypto__price_spread`
- 자산×분 grain. cross-check 결과.
- **Grain**: `(asset_code, ts_minute_utc)`

| 컬럼 | 의미 |
|---|---|
| `asset_code, ts_minute_utc` | PK |
| `n_sources_active` | 해당 분에 데이터 들어온 소스 수 |
| `price_min, price_max, price_median` | 소스 간 통계 |
| `spread_abs` | `price_max - price_min` |
| `spread_pct` | `spread_abs / price_median` |
| `is_anomaly` | 룰 적용 결과 |

#### `fct_crypto__anomaly_event`
- 룰 평가 결과 1건 = 1 row. **순수 감지 사실** (통지 정책과 분리).
- **Grain**: `(asset_code, ts_minute_utc, rule_code)` — 1분·1자산·1룰당 1 row
- **멱등**: 입력(price + 룰 정의)의 결정론적 함수 → `dbt run --full-refresh` 안전

| 컬럼 | 의미 |
|---|---|
| `event_id` | PK. 해시 = `MD5(asset_code‖ts_minute_utc‖rule_code)` |
| `asset_code` | |
| `ts_minute_utc` | 룰이 fire된 분 |
| `rule_code` | `'spread_over_threshold'`, `'source_stale'`, `'price_jump'` 등 |
| `severity` | `'info'`/`'warn'`/`'critical'` |
| `payload` | JSON. 룰별 상세(어떤 소스가, 얼마나 차이 났는지) |
| `detected_at` | 룰 평가 시각 (dbt run 시각) |

#### `fct_crypto__notification_dispatch`
- Slack webhook 1회 발사 = 1 row. **비멱등** (실제 외부 호출 기록이라 재처리 X).
- **Grain**: `dispatch_id`
- dedup·cooldown 정책은 여기서만 적용 → 정책 변경 시 `anomaly_event` history는 그대로

| 컬럼 | 의미 |
|---|---|
| `dispatch_id` | PK (UUID) |
| `asset_code` | |
| `rule_code` | |
| `severity` | 발사 시점의 severity |
| `dispatched_at` | Slack 발송 시각 |
| `event_ids` | ARRAY&lt;STRING&gt; — 이번 발사로 묶인 `anomaly_event.event_id` 들 |
| `message_body` | 실제 Slack 메시지 텍스트 (감사·재현용) |

**Dedup 룰 (cooldown):**
- 같은 `(asset_code, rule_code)`: cooldown **10분** (warn 기본)
- `severity='critical'`: cooldown **5분**
- severity 상승 (warn → critical): cooldown 무시, **즉시 발사**

---

## 5. 차원 테이블

#### `dim_asset`
| 컬럼 | 예시 |
|---|---|
| `asset_code` (PK) | `'BTCKRW'` |
| `base_ccy` | `'BTC'` |
| `quote_ccy` | `'KRW'` |
| `market_type` | `'crypto'` / `'fx'` / `'equity'` |
| `is_active` | true |

#### `dim_source`
| 컬럼 | 예시 |
|---|---|
| `source_code` (PK) | `'upbit'` |
| `source_name` | `'Upbit'` |
| `api_type` | `'rest'` / `'scrape'` |
| `rate_limit_per_min` | 600 |
| `home_url` | |

#### `dim_market_calendar` *(v0.3에서 equity 들어올 때 추가)*
- Grain: `(asset_code, ts_minute_utc)`, `is_market_open BOOL`
- crypto는 항상 open이므로 v0.1에선 불필요.

---

## 6. ERD (v0.1, crypto 도메인)

```
                ┌─────────────┐         ┌─────────────┐
                │ dim_asset   │         │ dim_source  │
                │ asset_code  │         │ source_code │
                └──────┬──────┘         └──────┬──────┘
                       │                       │
                       │ FK                    │ FK
                       │                       │
   ┌───────────────────┴───────────────────────┴────────────────┐
   │            stg_upbit__price_tick                            │
   │            stg_bithumb__price_tick                          │
   │   (asset_code, source_code, ts_utc) PK                      │
   └──────────────────────────┬──────────────────────────────────┘
                              │ UNION + 1분 집계
                              ▼
              ┌───────────────────────────────────┐
              │ int_crypto__price_tick_long       │
              │ (asset, source, ts_minute_utc) PK │
              └──────────────┬────────────────────┘
                             │
              ┌──────────────┼─────────────────────┐
              ▼              ▼                     ▼
   ┌──────────────────┐  ┌─────────────────────┐  ┌────────────────────────┐
   │ fct_crypto__     │  │ vw_crypto__         │  │ fct_crypto__           │
   │ price_minute_long│  │ price_minute_wide   │  │ price_spread           │
   │ (소스별 raw)     │  │ (소스 pivot)        │  │ (asset, ts_minute) PK  │
   └──────────────────┘  └─────────────────────┘  └───────────┬────────────┘
                                                              │ 룰 적용
                                                              ▼
                                                ┌─────────────────────────┐
                                                │ fct_crypto__            │
                                                │ anomaly_event           │
                                                │ (asset,ts_min,rule) PK  │
                                                │ 멱등                    │
                                                └────────────┬────────────┘
                                                             │ cooldown·dedup
                                                             ▼
                                                ┌─────────────────────────┐
                                                │ fct_crypto__            │
                                                │ notification_dispatch   │
                                                │ dispatch_id PK          │
                                                │ 비멱등                  │
                                                └────────────┬────────────┘
                                                             │
                                                             ▼ Slack webhook
```

---

## 7. Cross-check 룰 초안 (BTCKRW 기준, PoC)

> v0.1은 임계값 임시. 2주차 실데이터 분포 보고 재조정.

| rule_code | 정의 | 임계값 (초안) | severity |
|---|---|---|---|
| `spread_over_threshold` | `spread_pct > X` 가 N분 연속 | X=0.5%, N=2 | warn |
| `spread_critical` | `spread_pct > Y` | Y=2.0% (즉시) | critical |
| `source_stale` | 특정 소스의 `tick_count = 0` 이 N분 연속 | N=5 | warn |
| `all_sources_stale` | 자산의 모든 소스가 stale | N=2 | critical |
| `price_jump_single_source` | 단일 소스가 1분 내 `|Δprice|/price > Z%` | Z=1.0% (BTC), 별도 보정 필요 | info |

---

## 8. 결정 사항 / 미정 사항 <a id="decisions"></a>

### 확정 (v0.1)
- [x] 도메인별 fact 분리 (crypto / fx / equity)
- [x] spread는 long+pivot view로
- [x] raw 별도 보관 (JSON), stg에 raw_payload 미포함
- [x] PoC 자산: BTCKRW, 소스: Upbit + Bithumb
- [x] **D1. 1분 내 다중 tick 집계 = last tick(close)** — REST `/v1/ticker`가 어차피 마지막 체결가 스냅샷이라 자연스럽고, 거래소 분봉 close와 1:1 일치해서 디버깅 용이. 1분 1호출 현 스펙에선 tick 1개라 last/avg/vwap 결과 동일 → 미래에 호출 빈도 ↑ 또는 WebSocket 전환해도 정책 호환. WebSocket 전환 시 OHLCV 4종으로 컬럼 확장, `close`를 동일 역할로 유지.
- [x] **D2. ts_utc = 거래소 `trade_timestamp` 우선, 없으면 `ingested_at` fallback. `ingested_at`은 별도 컬럼으로 항상 저장.** — 정합성 비교의 본질은 "거래소가 말하는 체결 시각" 기준. 두 시각을 모두 보존해 사후 분석(네트워크 지연·collector 큐 지연) 가능. clock skew(D2-a)는 v0.1 무시(임계값 0.5% spread는 ms 어긋남 영향 없음), v0.2+에서 round-trip 측정 검토. 소스별 컬럼명(Upbit `trade_timestamp` / Bithumb `date`)은 stg에서 `ts_utc`로 통일.
- [x] **D3. 환산 없이 자산 코드 내에서만 cross-check.** — BTCKRW끼리(Upbit↔Bithumb), BTCUSDT끼리(Binance↔Bybit)만 비교. KRW↔USDT 환산 비교(=김치 프리미엄)는 **v0.2 fx 도메인 들어온 뒤 마일스톤 2편**으로 분리. 근거: ① crypto v0.1 → fx v0.2 단계화와 일관 ② USDKRW collector 실패가 crypto cross-check까지 무너뜨리는 의존성 회피 ③ 김치 프리미엄을 v0.2 셀링 포인트로 아끼면 블로그·포트폴리오 시리즈 구성 자연스러움.
- [x] **D4. 이벤트 dedup = 감지/통지 분리 (옵션 C).** `fct_crypto__anomaly_event`(grain=자산×분×룰, 멱등)와 `fct_crypto__notification_dispatch`(grain=`dispatch_id`, 비멱등)를 별개 fact로 둠. cooldown(warn 10분, critical 5분, severity 상승 시 무시)은 dispatch 쪽에서만 적용. 근거: ① 감지(결정론적)와 통지(정책 의존)는 grain·생애주기·재처리 가능성이 다름 ② cooldown 정책 변경 시 `anomaly_event` history 일관성 보존 ③ 한 테이블에 멱등/비멱등 섞이면 `dbt run --full-refresh` 안전성 깨짐 ④ Grafana Alerting · Prometheus Alertmanager · PagerDuty 동일 패턴 (rule eval ↔ alert routing 분리).
- [x] **D5. 알림 채널 = Slack 개인 워크스페이스 webhook.** 회사 `data-pipeline` 알림 표준([[project_data_pipeline_alerting]])과 동일 인터페이스(`[{project}][{severity}]` prefix, `SLACK_WEBHOOK_URL` env, 실패 시 본인 @멘션). 회사·개인 사프 양쪽에 동일 알림 표준 적용 → 토스 DAE 어필 시 "표준화된 알림 인터페이스" 한 줄로 정리. 워크스페이스만 분리(회사 vs `gihun-sideprojects`). 향후 모든 개인 사이드 프로젝트의 알림 채널 표준으로 채택.

### 결정 필요
- (없음 — v0.1 데이터 모델 1차 확정 완료)

---

## 9. 다음 단계 (작업 순서)

1. **이 문서 리뷰·확정** ← 현재
2. `dim_asset`, `dim_source` seed CSV 작성 (dbt seeds)
3. Upbit/Bithumb 응답 실제 호출 → JSON 샘플 캡처 → `raw_*` 스키마 확정
4. Docker Compose로 ClickHouse 1개 띄우기 (MergeTree + LowCardinality, k3s 미사용)
5. collector Python 스크립트 1개 (Upbit, 1분 cron)
6. dbt project init + `stg_upbit__price_tick` 1개 모델
7. Bithumb collector 추가 → `int_crypto__price_tick_long` 첫 빌드
8. `fct_crypto__price_spread` + 룰 1개(`spread_over_threshold`)
9. Slack webhook + `fct_crypto__anomaly_event` → `fct_crypto__notification_dispatch` 알림 발사 (cooldown 적용)
10. → **마일스톤 1 달성 (블로그 1편 거리)**