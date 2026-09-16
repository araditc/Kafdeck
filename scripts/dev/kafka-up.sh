#!/usr/bin/env bash
set -euo pipefail
docker compose -f deploy/dev/docker-compose.kafka.yml up -d
echo "Kafka development broker is starting on localhost:9092."
