"""fct_crypto__notification_dispatch 의 미발송 알림을 Slack으로 전송.

기본은 dry-run(페이로드 출력만). 실제 발송은 --send 플래그.
미발송 추적: watermark 파일(마지막 발송 dispatched_at 기록) — 사프라 단순하게.
회사 data-pipeline 알림과 동일 인터페이스: severity별 이모지 + critical은 @here 멘션 (D5).
"""

import argparse
import json
import logging
import os
from pathlib import Path

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

WATERMARK = Path(__file__).parent / ".slack_watermark"

SEVERITY_STYLE = {
    "critical": {"emoji": "🔴", "prefix": "<!here> ", "label": "CRITICAL"},
    "warn": {"emoji": "⚠️", "prefix": "", "label": "WARN"},
}


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "price_integrity"),
    )


def read_watermark() -> str:
    if WATERMARK.exists():
        return WATERMARK.read_text().strip()
    return "1970-01-01 00:00:00"


def write_watermark(ts: str) -> None:
    WATERMARK.write_text(ts)


def fetch_pending(client, since: str):
    """watermark 이후에 결정된 발송 대상만 조회."""
    q = """
        SELECT source_asset_code,
               source_ts_minute_utc,
               rule_code,
               severity,
               spread_pct,
               toString(dispatched_at) AS dispatched_at
        FROM fct_crypto__notification_dispatch
        WHERE dispatched_at > {since:String}
        ORDER BY dispatched_at
    """
    return client.query(q, parameters={"since": since}).result_rows


def build_message(row) -> dict:
    asset, ts_minute, rule, severity, spread_pct, _ = row
    style = SEVERITY_STYLE.get(severity, SEVERITY_STYLE["warn"])
    text = (
        f"{style['prefix']}{style['emoji']} [{style['label']}] 시세 정합성 이상 감지\n"
        f"• 자산: {asset}\n"
        f"• 시각(UTC): {ts_minute}\n"
        f"• 룰: {rule}\n"
        f"• 소스 간 괴리율: {spread_pct * 100:.4f}%"
    )
    return {"text": text}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--send", action="store_true", help="실제 Slack 발송 (미지정 시 dry-run)")
    args = parser.parse_args()

    client = get_client()
    since = read_watermark()
    rows = fetch_pending(client, since)

    if not rows:
        log.info("발송 대상 없음 (watermark=%s 이후)", since)
        return

    log.info("발송 대상 %d건 (watermark=%s 이후)", len(rows), since)

    if not args.send:
        for row in rows:
            print(json.dumps(build_message(row), ensure_ascii=False, indent=2))
        log.info("[dry-run] 실제 발송 안 함. 발송하려면 --send")
        return

    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook:
        log.error("SLACK_WEBHOOK_URL 미설정 — .env 확인")
        return

    max_ts = since
    for row in rows:
        resp = httpx.post(webhook, json=build_message(row), timeout=10)
        resp.raise_for_status()
        max_ts = max(max_ts, row[5])
        log.info("발송 OK: %s %s %s", row[0], row[3], row[1])

    write_watermark(max_ts)
    log.info("완료. watermark=%s 로 갱신", max_ts)


if __name__ == "__main__":
    main()
