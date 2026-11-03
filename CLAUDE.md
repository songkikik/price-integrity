# price-integrity

여러 거래소에서 동일 자산(BTC/KRW)의 시세를 1분 단위로 수집해 **소스 간 가격 괴리(spread)와 이상치(anomaly)를 감지**하는 사이드 프로젝트.

- **목적**: ① 토스 DAE 이직 어필(강점 = 데이터 정합성) ② ClickHouse / 실시간 수집 파이프라인 학습.
- **성격**: 운영 아님. **"받고 싶을 때만" 수동으로 돌리는 학습용.** 컨테이너 자동재시작·상시가동 설정 안 함.

## 아키텍처

```
 [수집=producer]        [버퍼]            [적재=consumer]      [변환 = dbt 4-layer]              [알림]
 ws_collector.py  ──> Kafka ──────────> kafka_to_clickhouse ─> raw ─> stg ─> int ─> marts ─> Slack
 (WS push, 2소스)     crypto.ticker.v1   (at-least-once)       (JSON) (정규화) (분집계) (spread)  webhook
                      파티션 3·key=source_code                  └─ ClickHouse (Docker) ─┘
```

**v0.3 디커플링**: 수집과 적재가 별개 프로세스. ClickHouse가 죽어도 수집은 계속되고(토픽에 쌓임),
적재만 다시 돌리고 싶으면 오프셋을 되감아 replay 한다. producer만 늘리면 소스 추가도 무중단.

| 계층 | 산출물 | 책임 | materialization |
|---|---|---|---|
| 수집 | `collectors/ws_collector.py` | 거래소 WS push → sink(Kafka 또는 ClickHouse)에 batch write | Python (asyncio) |
| 버퍼 | Kafka `crypto.ticker.v1` | envelope 보관 = 수집/적재 분리점. key=source_code로 소스별 순서 보장 | 파티션 3 · RF 1 |
| 적재 | `loader/kafka_to_clickhouse.py` | consumer → raw에 batch insert. 수동 오프셋 커밋(at-least-once) | Python (asyncio) |
| raw | `raw_ws__ticker` | 원본 JSON 보존 (파싱 안 함) | ClickHouse table |
| staging | `stg_ws__ticker_{upbit_fmt,coinone,korbit}` → `stg_ws__price_tick` | 포맷별 JSON 파싱 → union + grain dedup | view |
| intermediate | `int_crypto__price_tick_long` | 분당 last tick 채택 | view |
| marts | `fct_crypto__price_spread` 등 | 분×자산 grain spread·anomaly·notification | table |

## 스택 · 실행법

- **DW**: ClickHouse (Docker, `clickhouse_data` named volume로 영속). HTTP 8123 / native 9000.
- **버퍼**: Kafka 3.9 KRaft 단일 브로커 (Docker, `kafka_data` volume). PLAINTEXT 9092.
- **변환**: dbt-clickhouse 1.10.x. **Python 3.12 핀** (uv).
- **재개 명령**:
  ```bash
  cd ~/projects/price-integrity && docker compose up -d      # 볼륨 유지 = 데이터 보존
  ./scripts/kafka-init/create_topics.sh                      # 토픽 생성 (멱등, 최초 1회면 충분)

  # v0.3 경로: producer / consumer 를 각각 띄운다 (별개 프로세스 = 디커플링)
  uv run python collectors/ws_collector.py --sink kafka &
  uv run python loader/kafka_to_clickhouse.py &

  # v0.2 롤백 경로 (Kafka 없이 ClickHouse 직결)
  # uv run python collectors/ws_collector.py

  # dbt
  cd dbt && uv run dbt build --profiles-dir .
  ```
- **운영 확인 명령** (Kafka):
  ```bash
  # lag = 아직 적재 안 된 메시지 수. 0이면 consumer가 따라잡은 상태.
  docker exec price-integrity-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group ch-loader
  # replay = 오프셋 되감기 (consumer 정지 상태에서만 가능). --dry-run 으로 먼저 확인할 것.
  docker exec price-integrity-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group ch-loader --topic crypto.ticker.v1 \
    --reset-offsets --to-earliest --execute
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
| **v0.3** | 수집·저장 분리 | ✅ collector=**producer** → Kafka 토픽 → loader=**consumer** → ClickHouse. offset·replay 학습 완료. ✅ 소스 확장 Coinone·Korbit 추가(4소스, loader·int·mart 무수정). 🔜 Gopax(최대 5개) |

- **스케일업 도메인 결정 보류**: 글로벌 호가창 arbitrage(정합성 심화, 추천) vs 항공기 추적(정합성 이탈=별도 프로젝트) → 마일스톤 1 완성 후 판단.
- **제외**: v0.4 k8s(k3s)/Airflow — "24/7 상시가동·셀프힐"용이라 "받고 싶을 때만" 성격과 정반대.

## 현재 상태 (2026-11-04)

- ✅ **v0.3 5단계 = 무중단 소스 추가 (Coinone·Korbit → 4소스).** loader·Kafka 토픽·int·mart **0줄 수정**.
  - 스펙: 둘 다 무인증 public WS, **Upbit 표준 포맷 아님** → `stg_ws` 단일 모델 전제 붕괴 → **B안 채택**:
    포맷별 stg(`stg_ws__ticker_upbit_fmt` / `_coinone` / `_korbit`) + `stg_ws__price_tick`은 **이름 유지한 union+dedup 모델**.
    (버린 안: A=collector에서 Upbit 포맷 변환 → raw 원본 보존 깨짐 / C=단일 stg `multiIf` 분기 → 소스별 테스트 불가)
  - collector: `EXCHANGES`를 소스별 설정(url·subscribe·app_ping)으로. **Coinone은 앱 레벨 PING 필수**
    (`{"request_type":"PING"}`, 마지막 PING 후 30분 idle 끊김 → 10분 주기). 비데이터 메시지도 raw에 원문 적재, 필터는 stg.
  - 필드 매핑(단위 기준): 가격 coinone=`data.last`·korbit=`data.close` (둘 다 STRING) / 24h 거래량(BTC)
    coinone=`data.target_volume`·korbit=`data.volume`. ⚠️ `quote_volume`·`quoteVolume`은 **KRW 금액**이라 금지.
  - **시각 정의 차이(문서화)**: korbit=`lastTradedAt`(체결시각, 샘플 79/79 일치 → D2 부합).
    coinone=`data.timestamp`는 **ticker 이벤트 시각**(91.5% 체결시각과 동일, 나머지는 분 경계·호가 변화 재발행)
    → 가격은 항상 직전 체결가지만 **체결 없는 분에도 carry-forward 행이 생길 수 있음** (D2와의 의도된 차이).
  - 신규 singular test `assert_stg_ws_no_ghost_tick`: JSONExtract*는 파싱 실패 시 0 반환 → price≤0 / ts<2020 차단.
  - **검증(2026-11-03, 10분 Kafka E2E)**: lag 0 · stg grain 중복 0 · dbt PASS=95/ERROR=0 ·
    10분 전부 n_sources_active=4 · spread 0.049~0.1037% · 소스별 분 median 대비 편차 최대 6.43bp · anomaly/dispatch 0행.
  - **회귀**: 동점 그룹 제외 시 변경 전/후 stg 행 해시 완전 일치(upbit 7,030 / bithumb 2,836), int 기준값 동일.
  - ⚠️ **발견(기존 버그, 미수정)**: stg dedup `order by ingested_at desc`는 **동점 시 비결정적**.
    `ingested_at`이 ms 정밀도라 같은 ms 다중 체결(upbit 129그룹·393행)이 같은 키 → 실행계획 따라 다른 행 채택.
    결정적 tie-breaker(예: payload 해시·메시지 timestamp) 추가는 별도 작업으로 분리.
  - ⚠️ Kafka 파티션 쏠림: 4개 key가 전부 파티션 0·1로 해시 → 파티션 2 비어 있음. 정합성 무관(소스별 순서는 유지), 부하만 편중.
- 🔜 **다음**: (a) dedup tie-breaker 결정화 (b) Gopax 추가(5번째 소스) (c) replay 시 raw 비대 정리(`ReplacingMergeTree`).

## 이전 상태 (2026-09-23)

- ✅ **v0.3 Kafka 디커플링 완성 (0~4단계).** dbt **0줄 수정**으로 통과 = 회귀 기준점 유지.
  - 구성: Kafka 3.9 KRaft 단일 브로커 / 토픽 `crypto.ticker.v1`(파티션 3·RF 1·auto-create off) /
    envelope `{source_code, ingested_at, payload}` = `raw_ws__ticker` 3컬럼과 1:1 동형(그래서 dbt 무수정).
  - `ws_collector.py`에 **sink 분기** 추가(`--sink kafka|clickhouse`). 연결관리·버퍼 로직은 그대로 재사용,
    기본값 `clickhouse` = v0.2 경로가 롤백 수단으로 남음.
  - `loader/kafka_to_clickhouse.py` 신규: `enable_auto_commit=False` + **insert → commit 순서 = at-least-once**.
    근거는 "유실은 비가역, 중복은 `stg_ws`의 `qualify row_number()` dedup이 흡수".
  - **검증(2026-09-23)**: 2만여 건 E2E. lag 0 · stg grain 중복 0 · dbt PASS=79/ERROR=0 ·
    167분 중 164분 n_sources_active=2 · spread 0~0.1083% · anomaly/dispatch 0행(운영임계 0.5% 대비 정합).
  - **replay 실증**: 오프셋 `--to-earliest` 되감아 재적재 → raw 41,920→62,871(+20,951 중복)인데
    **stg 10,257 동일 · spread_pct 합계 0.0625834354 소수 10자리까지 동일**. at-least-once + 하류 멱등 증명.
  - ⚠️ 검증 함정: `stg`는 view(조회 시점 계산), `spread`는 table(build 시점 고정) → 레이어마다 시점이 다름.
    정합성 비교는 **모든 유입 정지 → build 1회 → 스냅샷** 순서를 지킬 것. (1차 실험이 이것 때문에 어긋남)
  - ⚠️ 미정리: replay 할 때마다 raw가 2만 행씩 붐. 실무라면 `ReplacingMergeTree`나 구간 삭제 절차 필요 —
    학습용이라 의도적으로 방치 중.
- (→ 2026-11-04 v0.3 5단계로 완료) 무중단 소스 추가(Coinone/Korbit WS 스펙 조사 선행).

## 이전 상태 (2026-08-30)

- ✅ **마일스톤 1(v0.1) 완성·검증.** anomaly_event·notification_dispatch·slack_notify 전부 구현. Upbit↔Bithumb BTC/KRW는 분 단위로 **매우 정합적**(괴리 0.002~0.052%) → 실데이터 anomaly 자연 발생 안 함.
- ✅ **v0.2 WebSocket 전환 완성 (0~6단계).** REST 폴링 → WS push. int/mart 로직 무수정·FROM만 교체 = 회귀 기준점(논점③) 지켜짐.
  - 스펙: 둘 다 무인증, Upbit=`api.upbit.com/websocket/v1`·Bithumb 2.x=`ws-api.bithumb.com/websocket/v1` **동일 포맷(Upbit 표준)**, 120초 idle timeout, `ticker` 타입.
  - 산출물: `ws_collector.py`(asyncio 2소스, ping keepalive+지수 백오프, 혼합 flush=size500 OR time5s) / `raw_ws__ticker`(단일+source_code) / `stg_ws__price_tick`(두 소스 1모델 파싱, source_code는 raw 컬럼 유래) / `int` FROM→stg_ws 전환.
  - 검증: 라이브 적재 OK, stg_ws 9컬럼·grain중복0·test 9 PASS, fct_spread n_sources=2·spread 0.07%대·anomaly 0(정합).
  - REST stg_upbit/bithumb·collector = **롤백용 보존(미참조)**, 별도 cleanup 대상.
- 🔜 **다음**: (a) 5b 강검증=REST·WS 동시가동 값 대조(선택) (b) `vw_crypto__price_minute_wide` pivot view (c) `trade` 타입·OHLCV 확장 → 그다음 **v0.3 Kafka**.
- 미해결(사프라 보류): should_flush는 메시지 도착 시에만 평가 → 완전 무음 구간 시간조건 지연. 정석은 별도 타이머 task(v0.3 Kafka가 대체).
