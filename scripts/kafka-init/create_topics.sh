#!/usr/bin/env bash
# v0.3 Kafka 토픽 생성 (멱등 — 이미 있으면 통과).
#
# auto-create를 껐기 때문에(docker-compose: KAFKA_AUTO_CREATE_TOPICS_ENABLE=false)
# 토픽은 반드시 여기서 명시 생성한다. auto-create에 맡기면 파티션 1개로 만들어져
# "파티션 3 + key 기반 라우팅" 설계가 조용히 무너진다.
#
# 실행: ./scripts/kafka-init/create_topics.sh

set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-price-integrity-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-crypto.ticker.v1}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"

# --if-not-exists: 이미 있으면 에러 없이 통과 → 재실행 안전
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor 1

echo "--- $TOPIC ---"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --describe --topic "$TOPIC"
