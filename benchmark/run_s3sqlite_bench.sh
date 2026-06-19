#!/bin/bash
set -euo pipefail
SIZE="${1:-100000}"
LOCAL_DB="/data/social_${SIZE}.db"

# Extract --page-size from remaining args so the local DB matches the VFS.
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
  echo "[s3sqlite-wrapper] generating local SQLite file: $LOCAL_DB"
  generate_social_db.py --size "$SIZE" --page-size "$PAGE_SIZE" --output "$LOCAL_DB"
else
  EXISTING_PAGE_SIZE=$(python3 -c "import sqlite3; print(sqlite3.connect('$LOCAL_DB').execute('PRAGMA page_size').fetchone()[0])")
  if [ "$EXISTING_PAGE_SIZE" != "$PAGE_SIZE" ]; then
    echo "[s3sqlite-wrapper] existing DB page size ($EXISTING_PAGE_SIZE) != requested ($PAGE_SIZE); regenerating"
    rm -f "$LOCAL_DB"
    generate_social_db.py --size "$SIZE" --page-size "$PAGE_SIZE" --output "$LOCAL_DB"
  fi
fi

# Drop --page-size N from the extra args; the harness only needs --queries, etc.
EXTRA_ARGS=("${@:2}")
FILTERED_ARGS=()
i=0
while [ $i -lt ${#EXTRA_ARGS[@]} ]; do
  arg="${EXTRA_ARGS[$i]}"
  if [ "$arg" = "--page-size" ]; then
    i=$((i+2))
    continue
  fi
  FILTERED_ARGS+=("$arg")
  i=$((i+1))
done

echo "[s3sqlite-wrapper] running s3sqlite benchmark"
exec /usr/local/bin/bench_s3sqlite.py --local-db "$LOCAL_DB" --s3-key "s3sqlite_social_${SIZE}.db" "${FILTERED_ARGS[@]}"

