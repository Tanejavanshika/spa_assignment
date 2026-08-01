#!/usr/bin/env bash
# UrbanPulse repository cleanup script
# Removes generated artifacts while preserving runtime directories.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> UrbanPulse cleanup starting in $ROOT"

# Python caches
find . -type d -name "__pycache__" -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -not -path "./.venv/*" -delete 2>/dev/null || true

# OS artifacts
find . -name ".DS_Store" -delete 2>/dev/null || true

# Runtime logs and Spark/Flink checkpoints (regenerated on next run)
rm -rf logs/* checkpoints/* 2>/dev/null || true
mkdir -p logs checkpoints
touch logs/.gitkeep checkpoints/.gitkeep

# Duplicate DLQ reports at repo root (canonical copies live in reports/)
rm -f dlq_report.md dlq_report.csv dlq_report_bar.png dlq_report_pie.png 2>/dev/null || true

# Temporary report logs can be regenerated; keep PDF deliverables
find reports -maxdepth 1 -type f -name "*.log" -delete 2>/dev/null || true

echo "==> Cleanup complete."
echo "    Preserved: source code, data/, reports/*.pdf, architecture/, docker/"
echo "    Removed:   __pycache__, logs/*, checkpoints/*, root DLQ duplicates"
