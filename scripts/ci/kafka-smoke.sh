#!/usr/bin/env bash
set -euo pipefail

compose_file="deploy/dev/docker-compose.kafka.yml"
cleanup() {
  docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$compose_file" up -d

for _ in {1..60}; do
  status="$(docker inspect --format='{{.State.Health.Status}}' kafdeck-kafka 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  sleep 2
done

if [[ "$(docker inspect --format='{{.State.Health.Status}}' kafdeck-kafka 2>/dev/null || true)" != "healthy" ]]; then
  docker compose -f "$compose_file" logs kafka
  echo "Kafka did not become healthy." >&2
  exit 1
fi

kafka_topics='/opt/kafka/bin/kafka-topics.sh'
docker exec kafdeck-kafka "$kafka_topics" --bootstrap-server localhost:9092 --create --topic kafdeck-ci-smoke --partitions 1 --replication-factor 1
docker exec kafdeck-kafka "$kafka_topics" --bootstrap-server localhost:9092 --describe --topic kafdeck-ci-smoke
docker exec kafdeck-kafka "$kafka_topics" --bootstrap-server localhost:9092 --delete --topic kafdeck-ci-smoke

echo "Kafka 4.3.1 KRaft smoke test passed."
