#!/bin/bash
set -euo pipefail
# Generate the local SQLite file if it doesn't exist, then run the sqlite-s3vfs benchmark.
SIZE="${1:-100000}"
LOCAL_DB="/data/social_${SIZE}.db"

# Extract --page-size from remaining args so the local DB matches s3vfs block size.
PAGE_SIZE=65536
for ((i=2; i<=$#; i++)); do
  arg="${!i}"
  if [ "$arg" = "--page-size" ]; then
    next=$((i+1))
    PAGE_SIZE="${!next:-65536}"
    break
  fi
done

if [ ! -f "$LOCAL_DB" ]; then
  echo "[s3vfs-wrapper] generating local SQLite file: $LOCAL_DB"
  generate_social_db.py --size "$SIZE" --page-size "$PAGE_SIZE" --output "$LOCAL_DB"
fi

# Drop --page-size N from the extra args; bench_s3vfs.py uses it as block_size.
EXTRA_ARGS=("${@:2}")
FILTERED_ARGS=()
i=0
while [ $i -lt ${#EXTRA_ARGS[@]} ]; do
  arg="${EXTRA_ARGS[$i]}"
  if [ "$arg" = "--page-size" ]; then
    PAGE_SIZE="${EXTRA_ARGS[$((i+1))]:-65536}"
    i=$((i+2))
    continue
  fi
  FILTERED_ARGS+=("$arg")
  i=$((i+1))
done

echo "[s3vfs-wrapper] running sqlite-s3vfs benchmark"
exec /usr/local/bin/bench_s3vfs.py --sizes "$SIZE" --page-size "$PAGE_SIZE" "${FILTERED_ARGS[@]}"

