#!/usr/bin/env bash
set -e

GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8080"}
MASTER_KEY=${MASTER_KEY:-"dev-admin-key"}

echo "=========================================================="
echo "    LLM Gateway Benchmark Runner"
echo "=========================================================="
echo "Gateway URL: $GATEWAY_URL"

# Check Python 3
if command -v python3 &>/dev/null; then
    python3 "$(dirname "$0")/benchmark.py" --gateway-url "$GATEWAY_URL" --master-key "$MASTER_KEY" "$@"
else
    echo "Python 3 is required to run the automated benchmark script."
    exit 1
fi

