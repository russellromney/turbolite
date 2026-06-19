#!/bin/bash
set -euo pipefail
SIZE="${1:-100000}"
LOCAL_DB="/data/social_${SIZE}.db"

# Graft currently requires SQLite pages to match its internal 4KB page boundary,
# so we force a 4KB-page local DB regardless of the --page-size argument.
PAGE_SIZE=4096

# Always regenerate for Graft so we don't accidentally reuse a DB created with
# a different page size by another harness.
if [ -f "$LOCAL_DB" ]; then
  echo "[graft-wrapper] removing existing local DB to ensure ${PAGE_SIZE}-byte pages"
  rm -f "$LOCAL_DB"
fi

echo "[graft-wrapper] generating local SQLite file with 4KB pages: $LOCAL_DB"
generate_social_db.py --size "$SIZE" --page-size "$PAGE_SIZE" --output "$LOCAL_DB"

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

echo "[graft-wrapper] running Graft benchmark"
exec /usr/local/bin/bench_graft.py --local-db "$LOCAL_DB" --tag "social_${SIZE}" "${FILTERED_ARGS[@]}"

