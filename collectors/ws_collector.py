"""v0.2 WebSocket 실시간 시세 수집기 (ticker 타입).

REST 폴링(upbit_collector.py / bithumb_collector.py)을 대체.
Upbit·Bithumb 2.x WS는 구독 메시지·응답 포맷이 동일(Upbit 표준)이라 1개 코드로 두 소스 처리.

핵심 (WS라서 새로 생기는 것):
- 연결관리: 120초 idle timeout → 라이브러리 ping_interval로 keepalive + 지수 백오프 재접속.
- 적재: 체결마다 push(다발) → 메시지 1건마다 insert 금지(ClickHouse 파트 폭증).
  버퍼에 모아 batch flush. flush 전략은 TickerBuffer.should_flush()에서 결정 (TODO 나).

실행:
  uv run python collectors/ws_collector.py
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import websockets
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# 두 거래소 공통: WSS URL만 다르고 구독 메시지·응답 포맷은 동일(Upbit 표준).
EXCHANGES = {
    "upbit": "wss://api.upbit.com/websocket/v1",
    "bithumb": "wss://ws-api.bithumb.com/websocket/v1",
}
SUBSCRIBE = [
    {"ticket": "price-integrity"},
    {"type": "ticker", "codes": ["KRW-BTC"]},
    {"format": "DEFAULT"},
]

# 연결관리 파라미터
PING_INTERVAL = 60   # 서버 idle timeout 120초 → 그 전에 ping (라이브러리가 자동 송신)
PING_TIMEOUT = 10
BACKOFF_START = 1    # 재접속 지수 백오프 시작(초)
BACKOFF_MAX = 60

# 적재 batch flush 파라미터 (혼합: 크기 OR 시간, 먼저 도달하는 쪽)
FLUSH_ROWS = 500     # 크기 기준: 바쁠 때 꽉 채워 효율적 insert
FLUSH_SEC = 5        # 시간 기준: 조용할 때도 최대 5초 안엔 내려감(신선도·유실방지 안전망)


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "price_integrity"),
    )


class TickerBuffer:
    """수집한 ticker 메시지를 모아 ClickHouse에 batch insert.

    row = [ingested_at(datetime), source_code(str), raw_payload(str)]
    """

    COLUMNS = ["ingested_at", "source_code", "raw_payload"]

    def __init__(self, client):
        self.client = client
        self.rows: list[list] = []
        self.last_flush = time.monotonic()

    def add(self, row: list) -> None:
        self.rows.append(row)

    def should_flush(self) -> bool:
        """flush 시점 결정 = ClickHouse 적재 효율 vs 데이터 신선도 트레이드오프.

        혼합(size OR time) 전략 = 실무 표준(Kafka batch.size/linger.ms 등과 동형):
          - 크기(FLUSH_ROWS): 바쁠 때 먼저 걸림 → 꽉 채워 효율적 insert.
          - 시간(FLUSH_SEC):  조용할 때 안전망 → 최대 지연 상한 보장·유실 방지.
        둘 중 먼저 도달하는 쪽에서 flush.
        """
        return (
            len(self.rows) >= FLUSH_ROWS
            or (time.monotonic() - self.last_flush) >= FLUSH_SEC
        )

    def flush(self) -> None:
        if not self.rows:
            return
        self.client.insert("raw_ws__ticker", self.rows, column_names=self.COLUMNS)
        log.info("flush %d rows", len(self.rows))
        self.rows = []
        self.last_flush = time.monotonic()


async def consume(source_code: str, url: str, buffer: TickerBuffer) -> None:
    """한 거래소 WS에 연결·구독하고 메시지를 버퍼에 적재. 끊기면 백오프 후 재접속."""
    backoff = BACKOFF_START
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT
            ) as ws:
                await ws.send(json.dumps(SUBSCRIBE))
                log.info("%s 연결·구독 OK", source_code)
                backoff = BACKOFF_START  # 연결 성공 시 백오프 리셋
                async for message in ws:
                    # Upbit/Bithumb는 binary frame(UTF-8 JSON)로 응답.
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")
                    ingested_at = datetime.now(timezone.utc)
                    buffer.add([ingested_at, source_code, message])
                    if buffer.should_flush():
                        buffer.flush()
        except Exception as e:
            log.error("%s 연결 끊김: %s → %ds 후 재접속", source_code, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)  # 지수 백오프


async def main() -> None:
    log.info("WS collector 시작 (ticker, KRW-BTC, %s)", ", ".join(EXCHANGES))
    client = get_client()
    buffer = TickerBuffer(client)
    # 두 거래소는 버퍼를 공유(단일 raw 테이블). 각각 별도 task로 동시 수신.
    await asyncio.gather(
        *(consume(src, url, buffer) for src, url in EXCHANGES.items())
    )


if __name__ == "__main__":
    asyncio.run(main())
