#!/usr/bin/env bash
# Create UrbanPulse Kafka topics after the cluster is healthy.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
python3 kafka/create_topics.py
