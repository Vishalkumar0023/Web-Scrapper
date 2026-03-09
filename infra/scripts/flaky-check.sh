#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
API_DIR="$ROOT_DIR/apps/api"

REPEATS="${1:-5}"
TARGET="${2:-tests}"

if ! [[ "$REPEATS" =~ ^[0-9]+$ ]] || [[ "$REPEATS" -lt 1 ]]; then
  echo "REPEATS must be a positive integer. Got: $REPEATS"
  exit 2
fi

echo "[flaky-check] root: $ROOT_DIR"
echo "[flaky-check] api dir: $API_DIR"
echo "[flaky-check] repeats: $REPEATS"
echo "[flaky-check] target: $TARGET"

failures=0
for run in $(seq 1 "$REPEATS"); do
  echo "[flaky-check] run $run/$REPEATS"
  if ! (cd "$API_DIR" && python3 -m pytest "$TARGET" -q); then
    failures=$((failures + 1))
    echo "[flaky-check] run $run failed"
  fi
done

if [[ "$failures" -gt 0 ]]; then
  echo "[flaky-check] $failures of $REPEATS runs failed"
  exit 1
fi

echo "[flaky-check] all runs passed"
