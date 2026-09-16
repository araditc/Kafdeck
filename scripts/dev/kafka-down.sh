#!/usr/bin/env bash
set -euo pipefail
docker compose -f deploy/dev/docker-compose.kafka.yml down -v --remove-orphans
