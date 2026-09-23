# price-integrity

여러 거래소에서 동일 자산(BTC/KRW)의 시세를 1분 단위로 수집해 **소스 간 가격 괴리(spread)와 이상치(anomaly)를 감지**하는 사이드 프로젝트.

- **목적**: ① 토스 DAE 이직 어필(강점 = 데이터 정합성) ② ClickHouse / 실시간 수집 파이프라인 학습.
- **성격**: 운영 아님. **"받고 싶을 때만" 수동으로 돌리는 학습용.** 컨테이너 자동재시작·상시가동 설정 안 함.

## 아키텍처

```
 [수집]                    [저장]              [변환 = dbt 4-layer]                     [알림]
 upbit_collector.py  ─┐                 raw ──> staging ──> intermediate ──> marts
 (REST 60s 폴링)      ├─> ClickHouse ─>  (JSON)  (정규화)    (소스 union)    (spread/anomaly) ─> Slack
 bithumb_collector.py ┘   (Docker)                                                              webhook
```

| 계층 | 산출물 | 책임 | materialization |
|---|---|---|---|
| 수집 | `collectors/*.py` | 거래소 REST 폴링 → raw에 JSON append | Python 스크립트 |
| raw | `raw_{src}__ticker` | 원본 JSON 보존 (파싱 안 함) | ClickHouse table |
| staging | `stg_{src}__price_tick` | JSON → 공통 스키마 정규화 (타입·단위 통일) | view |
| intermediate | `int_crypto__price_tick_long` | 소스 UNION + 분당 last tick 채택 | view |
| marts | `fct_crypto__price_spread` 등 | 분×자산 grain spread·anomaly·notification | table |

## 스택 · 실행법

- **DW**: ClickHouse (Docker, `clickhouse_data` named volume로 영속). HTTP 8123 / native 9000.
- **변환**: dbt-clickhouse 1.10.x. **Python 3.12 핀** (uv).
- **재개 명령**:
  ```bash
  cd ~/projects/price-integrity && docker compose up -d      # 볼륨 유지 = 데이터 보존
  # collector (동시 가동해야 겹치는 분 확보됨)
  uv run python collectors/upbit_collector.py &
  uv run python collectors/bithumb_collector.py &
  # dbt
  cd dbt && uv run dbt run --profiles-dir . && uv run dbt test --profiles-dir .
  ```
- `.env` / `profiles.yml` / `.venv` 는 gitignore. profiles.yml은 env_var 기본값(localhost/default/빈비번)으로 `.env` 없이도 연결됨.

## ⚠️ 환경 함정 (재현 방지)

- **Python 3.14 = dbt 비호환**: uv가 시스템 최신 3.14를 잡으면 dbt import 시 `mashumaro.exceptions.UnserializableField`. → `.python-version`을 **3.12**로 핀 후 `.venv` 재생성.
- **dbt-clickhouse는 SELECT 컬럼명 중복 거부** (`Code:44 ILLEGAL_COLUMN`): 뷰에서 같은 컬럼 2번 select 금지.

## 데이터 모델 결정 (v0.1 확정)

- **D1**: 1분 내 다중 tick = **last tick(close)** 채택. WebSocket 전환 시 OHLCV 확장, close 역할 유지.
- **D2**: `ts_utc` = 거래소 `trade_timestamp` 우선, 없으면 `ingested_at` fallback. `ingested_at`은 항상 별도 보존.
- **D3**: **환산 없이 동일 자산 코드 내에서만 cross-check**. KRW↔USDT 환산(김치 프리미엄)은 v0.2 fx 도메인.
- **D4**: **감지/통지 분리**. `anomaly_event`(grain=자산×분×룰, **멱등**) vs `notification_dispatch`(grain=dispatch_id, **비멱등**). cooldown은 dispatch에서만.
- **D5**: 알림 = Slack 개인 워크스페이스 webhook (회사 data-pipeline 표준과 동일 인터페이스).

## 로드맵 (2026-07-06 확정 — v0.3까지 완주, "써보는 게 목적")

> **핵심 서사**: Kafka/WebSocket은 *규모가 아니라 학습 목적*. 4~5 producer는 처리량상 오버킬 → 면접 서사는 **producer/consumer 디커플링 · replay · 무중단 소스 추가** 패턴 학습.

| 단계 | 목표 | 작업 |
|---|---|---|
| **마일스톤 1** (v0.1) | end-to-end 완성 | ✅ 겹침·fct_spread 검증 → ③ `anomaly_event` → ④ `notification_dispatch` → Slack |
| **v0.2** | 진짜 실시간 수집 | collector REST 폴링 → **WebSocket push** 전환. 연결관리(재접속·heartbeat), tick 수집. 기존 stg/int/mart **출력 동일성 = 회귀 기준점** |
| **v0.3** | 수집·저장 분리 | collector=**producer** → Kafka 토픽 → loader=**consumer** → ClickHouse. offset·replay 학습. 소스 확장(Coinone·Korbit·Gopax, 최대 5개)으로 producer 다변화 |

- **스케일업 도메인 결정 보류**: 글로벌 호가창 arbitrage(정합성 심화, 추천) vs 항공기 추적(정합성 이탈=별도 프로젝트) → 마일스톤 1 완성 후 판단.
- **제외**: v0.4 k8s(k3s)/Airflow — "24/7 상시가동·셀프힐"용이라 "받고 싶을 때만" 성격과 정반대.

## 현재 상태 (2026-08-30)

- ✅ **마일스톤 1(v0.1) 완성·검증.** anomaly_event·notification_dispatch·slack_notify 전부 구현. Upbit↔Bithumb BTC/KRW는 분 단위로 **매우 정합적**(괴리 0.002~0.052%) → 실데이터 anomaly 자연 발생 안 함.
- ✅ **v0.2 WebSocket 전환 완성 (0~6단계).** REST 폴링 → WS push. int/mart 로직 무수정·FROM만 교체 = 회귀 기준점(논점③) 지켜짐.
  - 스펙: 둘 다 무인증, Upbit=`api.upbit.com/websocket/v1`·Bithumb 2.x=`ws-api.bithumb.com/websocket/v1` **동일 포맷(Upbit 표준)**, 120초 idle timeout, `ticker` 타입.
  - 산출물: `ws_collector.py`(asyncio 2소스, ping keepalive+지수 백오프, 혼합 flush=size500 OR time5s) / `raw_ws__ticker`(단일+source_code) / `stg_ws__price_tick`(두 소스 1모델 파싱, source_code는 raw 컬럼 유래) / `int` FROM→stg_ws 전환.
  - 검증: 라이브 적재 OK, stg_ws 9컬럼·grain중복0·test 9 PASS, fct_spread n_sources=2·spread 0.07%대·anomaly 0(정합).
  - REST stg_upbit/bithumb·collector = **롤백용 보존(미참조)**, 별도 cleanup 대상.
- 🔜 **다음**: (a) 5b 강검증=REST·WS 동시가동 값 대조(선택) (b) `vw_crypto__price_minute_wide` pivot view (c) `trade` 타입·OHLCV 확장 → 그다음 **v0.3 Kafka**.
- 미해결(사프라 보류): should_flush는 메시지 도착 시에만 평가 → 완전 무음 구간 시간조건 지연. 정석은 별도 타이머 task(v0.3 Kafka가 대체).
