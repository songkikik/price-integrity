"""v0.3 Kafka → ClickHouse 적재기 (consumer).

ws_collector --sink kafka 가 토픽에 넣은 envelope을 읽어 raw_ws__ticker에 batch insert.
수집(producer)과 저장(consumer)이 별개 프로세스가 되는 지점 = v0.3의 목적.
  - ClickHouse가 죽어도 수집은 계속된다(메시지는 토픽에 쌓임).
  - 적재만 다시 돌리고 싶으면 오프셋을 되감아 replay 하면 된다.

envelope = {"source_code": str, "ingested_at": ISO8601, "payload": WS 원본 JSON 문자열}
         → raw_ws__ticker의 3컬럼과 1:1 동형이라 파싱 없이 그대로 insert (dbt 무수정).

실행:
  uv run python loader/kafka_to_clickhouse.py
"""

import asyncio
import json
import logging
import os
from datetime import datetime

import clickhouse_connect
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

TABLE = "raw_ws__ticker"
COLUMNS = ["ingested_at", "source_code", "raw_payload"]

# 배치 파라미터 (collector의 혼합 flush와 같은 발상: 크기 OR 시간 중 먼저 걸리는 쪽)
BATCH_MAX_RECORDS = 500   # 한 번에 가져올 최대 메시지 수
BATCH_TIMEOUT_MS = 5000   # 이 시간 안에 안 차면 있는 만큼만 들고 나옴


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "price_integrity"),
    )


def to_row(message) -> list:
    """Kafka 메시지(envelope) → raw_ws__ticker 1행.

    payload는 문자열 그대로 둔다. 여기서 파싱하면 raw 레이어가 "원본 보존"이 아니게 되고,
    파싱 책임이 stg(dbt)에서 loader(Python)로 새어나간다.
    """
    envelope = json.loads(message.value.decode("utf-8"))
    return [
        datetime.fromisoformat(envelope["ingested_at"]),
        envelope["source_code"],
        envelope["payload"],
    ]


def insert_rows(client, rows: list[list]) -> None:
    """ClickHouse에 batch insert (동기)."""
    client.insert(TABLE, rows, column_names=COLUMNS)


async def flush_batch(consumer, client, rows: list[list]) -> None:
    """배치를 ClickHouse에 적재하고 Kafka 오프셋을 커밋한다.

    ─────────────────────────────────────────────────────────────────
    TODO(나): insert와 commit의 순서를 정해서 구현할 것.

    consumer는 enable_auto_commit=False 로 떠 있다. 즉 "여기까지 처리했다"를
    브로커에 알리는 시점을 네가 직접 고른다. 두 선택지가 있고, 고른 쪽이
    이 파이프라인의 전달 보장(delivery semantics)을 결정한다.

      (A) commit 먼저 → insert      = at-most-once
          insert 직전에 죽으면? 오프셋은 이미 넘어갔으므로 그 배치는 영원히 안 들어온다. → 유실
      (B) insert 먼저 → commit      = at-least-once
          commit 직전에 죽으면? 재시작 시 같은 오프셋부터 다시 읽어 같은 배치를 또 넣는다. → 중복

    판단 근거(이 프로젝트 한정):
      stg_ws__price_tick.sql 이 (asset_code, source_code, ts_utc) 기준으로
      qualify row_number() ... = 1 dedup을 이미 하고 있다.
      → 중복은 하류가 흡수하지만, 유실은 아무도 복구해주지 못한다.

    선택: (B) at-least-once.
      유실은 비가역이고 중복은 하류(stg dedup)가 흡수하므로, 둘 중 복구 가능한 쪽의
      실패를 택한다. insert가 예외로 죽으면 commit에 도달하지 못하고,
      재시작 시 같은 오프셋부터 다시 읽어 그 배치가 복구된다.
    ─────────────────────────────────────────────────────────────────
    """
    insert_rows(client, rows)
    await consumer.commit()


async def main() -> None:
    topic = os.environ.get("KAFKA_TOPIC", "crypto.ticker.v1")
    group = os.environ.get("KAFKA_CONSUMER_GROUP", "ch-loader")

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=group,
        # 커밋 시점을 직접 통제하려면 자동 커밋을 꺼야 한다 (flush_batch 참고).
        enable_auto_commit=False,
        # 이 group이 처음 붙을 때 어디서부터 읽을지. earliest = 토픽 맨 앞부터.
        auto_offset_reset="earliest",
    )
    await consumer.start()
    client = get_client()
    log.info("loader 시작 topic=%s group=%s → %s", topic, group, TABLE)

    try:
        while True:
            # getmany: 배치 단위로 가져옴. 크기(max_records) OR 시간(timeout_ms) 중 먼저 걸리는 쪽.
            batches = await consumer.getmany(
                timeout_ms=BATCH_TIMEOUT_MS, max_records=BATCH_MAX_RECORDS
            )
            rows = [to_row(msg) for msgs in batches.values() for msg in msgs]
            if not rows:
                continue
            await flush_batch(consumer, client, rows)
            log.info("적재 %d rows", len(rows))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
