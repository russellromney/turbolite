#!/usr/bin/env python3
"""Benchmark s3sqlite against the same social queries as tiered-bench.

s3sqlite reads a single SQLite file from S3 via s3fs + APSW. It is read-only.
We upload the local SQLite file to S3 once, then open fresh read-only
connections per iteration and time the queries.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import apsw
import s3fs
import s3sqlite

QUERIES = {
    "post": (
        "SELECT p.content, u.first_name, u.last_name "
        "FROM posts p JOIN users u ON p.user_id = u.id WHERE p.id = ?",
        lambda: phash(42 + 8_000_000) % 100000,
    ),
    "profile": (
        "SELECT u.id, u.first_name, u.last_name, u.school, u.city, "
        "(SELECT COUNT(*) FROM friendships f WHERE f.user_a = u.id OR f.user_b = u.id), "
        "(SELECT COUNT(*) FROM posts p WHERE p.user_id = u.id) "
        "FROM users u WHERE u.id = ?",
        lambda: phash(42 + 8_000_001) % 10000,
    ),
    "who-liked": (
        "SELECT u.first_name, u.last_name FROM likes l "
        "JOIN users u ON l.user_id = u.id WHERE l.post_id = ? LIMIT 50",
        lambda: phash(42 + 8_000_002) % 100000,
    ),
    "mutual": (
        "SELECT COUNT(*) FROM friendships a "
        "JOIN friendships b ON b.user_b = a.user_b "
        "WHERE a.user_a = ? AND b.user_a = ? AND a.user_a < b.user_a",
        lambda: (phash(42 + 8_000_003) % 10000, phash(42 + 8_000_004) % 10000),
    ),
    "idx-filter": (
        "SELECT COUNT(*) FROM posts WHERE user_id = ? AND created_at > ?",
        lambda: (phash(42 + 8_000_005) % 10000, 1075000000 + (phash(42 + 8_000_006) % 94000000)),
    ),
    "scan-filter": (
        "SELECT COUNT(*) FROM posts WHERE content LIKE ?",
        lambda: f"%{ ['amazing', 'brutal', 'project', 'dinner', 'concert'][phash(42 + 8_000_007) % 5] }%",
    ),
}


def phash(seed: int) -> int:
    x = seed & 0xFFFFFFFFFFFFFFFF
    x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    return x


def get_s3fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        anon=False,
        client_kwargs={
            "endpoint_url": os.environ.get("AWS_ENDPOINT_URL", os.environ.get("AWS_ENDPOINT", "")),
            "region_name": os.environ.get("AWS_REGION", "auto"),
        },
    )


def upload_if_missing(local_db: str, key: str) -> None:
    fs = get_s3fs()
    bucket = os.environ["TIERED_TEST_BUCKET"]
    s3_path = f"{bucket}/{key}"
    local_size = os.path.getsize(local_db)
    if fs.exists(s3_path):
        s3_size = fs.size(s3_path)
        if s3_size == local_size:
            print(f"[s3sqlite] S3 object already exists and matches size: s3://{bucket}/{key}")
            return
        print(f"[s3sqlite] S3 object size mismatch ({s3_size} != {local_size}); re-uploading")
    else:
        print(f"[s3sqlite] uploading {local_db} to s3://{bucket}/{key} ...")
    fs.put(local_db, s3_path)


def run_query_cold(key: str, sql: str, params, timeout: int | None = None) -> dict:
    fs = get_s3fs()
    bucket = os.environ["TIERED_TEST_BUCKET"]
    vfs = s3sqlite.S3VFS(name="s3sqlite-bench", fs=fs)
    start = time.perf_counter()
    with apsw.Connection(
        f"{bucket}/{key}",
        vfs=vfs.name,
        flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
    ) as conn:
        cursor = conn.cursor()
        if timeout:
            import signal

            def _handler(signum, frame):
                raise TimeoutError(f"query exceeded {timeout}s")

            signal.signal(signal.SIGALRM, _handler)
            signal.setitimer(signal.ITIMER_REAL, timeout)
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            finally:
                signal.alarm(0)
        else:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    elapsed = time.perf_counter() - start
    return {"elapsed_ms": elapsed * 1000.0, "rows": len(rows)}


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-db", required=True)
    parser.add_argument("--s3-key", default="s3sqlite_social_100000.db")
    parser.add_argument("--queries", default=",".join(QUERIES.keys()))
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--per-query-timeout", type=int, default=300)
    args = parser.parse_args()

    upload_if_missing(args.local_db, args.s3_key)

    query_names = [q.strip() for q in args.queries.split(",") if q.strip()]
    print(f"\n[s3sqlite] benchmark results (cold, fresh connection per iteration)")
    print(f"{'query':16} {'p50':>10} {'p95':>10} {'p99':>10}")

    for name in query_names:
        if name not in QUERIES:
            print(f"[s3sqlite] unknown query: {name}", file=sys.stderr)
            continue
        sql, param_fn = QUERIES[name]
        times = []
        for i in range(args.warmup + args.iterations):
            params = param_fn()
            if not isinstance(params, tuple):
                params = (params,)
            try:
                result = run_query_cold(args.s3_key, sql, params, args.per_query_timeout)
                if i >= args.warmup:
                    times.append(result["elapsed_ms"])
            except Exception as e:
                print(f"[s3sqlite] {name} error: {e}", file=sys.stderr)
                break
        if times:
            times.sort()
            print(f"  [s3sqlite] {name:13} {percentile(times, 50):>10.1f}ms {percentile(times, 95):>10.1f}ms {percentile(times, 99):>10.1f}ms")

    return 0


if __name__ == "__main__":
    sys.exit(main())
