#!/bin/bash
set -euo pipefail

# Lambda custom runtime bootstrap
# Runs tiered-bench and returns output as the Lambda response

while true; do
  # Get next invocation
  HEADERS=$(mktemp)
  EVENT_DATA=$(curl -sS -LD "$HEADERS" \
    "http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/next")
  REQUEST_ID=$(grep -Fi Lambda-Runtime-Aws-Request-Id "$HEADERS" | tr -d '[:space:]' | cut -d: -f2)
  rm -f "$HEADERS"

  echo "=== Lambda invocation $REQUEST_ID ===" >&2
  echo "BUCKET_NAME=$BUCKET_NAME" >&2
  echo "AWS_REGION=${AWS_REGION:-unset}" >&2
  echo "BENCH_REUSE=${BENCH_REUSE:-unset}" >&2

  # Run benchmark with 10 minute timeout, capture stdout+stderr
  OUTPUT=$(timeout 600 /var/task/tiered-bench 2>&1) && EXIT_CODE=$? || EXIT_CODE=$?
  echo "tiered-bench exit code: $EXIT_CODE" >&2

  if [ $EXIT_CODE -eq 124 ]; then
    OUTPUT="TIMEOUT after 600s. Partial output:\n$OUTPUT"
  fi

  # Escape for JSON (handle missing python3)
  if command -v python3 &>/dev/null; then
    ESCAPED=$(echo "$OUTPUT" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo '"escape failed"')
  else
    # Simple JSON escape without python
    ESCAPED=$(echo "$OUTPUT" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g' | awk '{printf "%s\\n", $0}' | sed 's/^/"/; s/$/"/')
  fi

  # Send response
  curl -sS -X POST \
    "http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/$REQUEST_ID/response" \
    -d "{\"output\": $ESCAPED, \"exit_code\": $EXIT_CODE}"
done
