#!/usr/bin/env bash
# Start the UrbanPulse Kafka cluster via Docker Compose.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/docker"
docker compose up -d
echo "Kafka cluster starting. Kafka UI: http://localhost:8080"
