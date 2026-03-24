#!/bin/bash
set -o pipefail
# Use mounted volume for temp/cache if available
if [ -d /data ]; then
  export TMPDIR=/data
fi

# Log output to S3 if bucket is configured
if [ -n "$TIERED_TEST_BUCKET" ]; then
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  MACHINE_ID=$(hostname || echo "unknown")
  LOG_FILE="/tmp/bench_${TIMESTAMP}_${MACHINE_ID}.log"
  S3_LOG_PATH="s3://${TIERED_TEST_BUCKET}/bench/logs/bench_${TIMESTAMP}_${MACHINE_ID}.log"

  # Background loop to upload progress every 30 seconds
  if command -v aws >/dev/null 2>&1; then
    (
      while sleep 30; do
        [ -f "$LOG_FILE" ] && aws s3 cp "$LOG_FILE" "$S3_LOG_PATH" ${AWS_ENDPOINT_URL:+--endpoint-url=$AWS_ENDPOINT_URL} 2>/dev/null || true
      done
    ) &
    UPLOADER_PID=$!
  fi

  # Run benchmark, tee output to file and stdout
  tiered-bench "$@" 2>&1 | tee "$LOG_FILE"
  EXIT_CODE=$?

  # Kill background uploader
  [ -n "$UPLOADER_PID" ] && kill $UPLOADER_PID 2>/dev/null || true

  # Upload final log to S3
  if command -v aws >/dev/null 2>&1; then
    aws s3 cp "$LOG_FILE" "$S3_LOG_PATH" ${AWS_ENDPOINT_URL:+--endpoint-url=$AWS_ENDPOINT_URL} 2>/dev/null || true
  fi

  exit $EXIT_CODE
else
  exec tiered-bench "$@"
fi
