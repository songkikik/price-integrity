import json
import logging
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

UPBIT_URL = "https://api.upbit.com/v1/ticker"


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "price_integrity"),
    )


def fetch_ticker() -> str:
    resp = httpx.get(UPBIT_URL, params={"markets": "KRW-BTC"}, timeout=10)
    resp.raise_for_status()
    return json.dumps(resp.json()[0], ensure_ascii=False)


def insert_raw(client, ingested_at: datetime, raw_payload: str) -> None:
    client.insert(
        "raw_upbit__ticker",
        [[ingested_at, raw_payload]],
        column_names=["ingested_at", "raw_payload"],
    )


def main() -> None:
    log.info("Upbit collector 시작 (1분 주기, KRW-BTC)")
    client = get_client()

    while True:
        ingested_at = datetime.now(timezone.utc)
        try:
            raw_payload = fetch_ticker()
            insert_raw(client, ingested_at, raw_payload)
            log.info("OK  ingested_at=%s", ingested_at.strftime("%H:%M:%S"))
        except Exception as e:
            log.error("FAIL %s", e)
        time.sleep(60)


if __name__ == "__main__":
    main()
