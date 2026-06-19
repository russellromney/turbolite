#!/usr/bin/env python3
"""
Cold-latency benchmark for sqlite-s3vfs.

Mirrors the turbolite tiered-bench queries and parameter generation so the
numbers are comparable. sqlite-s3vfs stores each SQLite page as a separate S3
object, so this is the "naive one-object-per-page" baseline.
"""

import argparse
import os
import statistics
import time
from typing import Callable, List

import apsw
import boto3
import sqlite_s3vfs


# ---------------------------------------------------------------------------
# Parameter generation: exact port of Rust phash() used by tiered-bench.rs
# ---------------------------------------------------------------------------
def phash(seed: int) -> int:
    x = seed & 0xFFFFFFFFFFFFFFFF
    x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    return x


def make_param_fn(n_posts: int, n_users: int, query: str) -> Callable[[int], tuple]:
    def post(i: int) -> tuple:
        pid = phash(i + 500) % n_posts
        return (pid,)

    def profile(i: int) -> tuple:
        uid = phash(i + 100) % n_users
        return (uid,)

    def who_liked(i: int) -> tuple:
        pid = phash(i + 200) % n_posts
        return (pid,)

    def mutual(i: int) -> tuple:
        a = phash(i + 300) % n_users
        b = phash(i + 400) % n_users
        return (a, b)

    def idx_filter(i: int) -> tuple:
        uid = phash(i + 600) % n_users
        return (uid,)

    def scan_filter(i: int) -> tuple:
        threshold = phash(i + 700) % 50
        return (threshold,)

    return {
        "post": post,
        "profile": profile,
        "who-liked": who_liked,
        "mutual": mutual,
        "idx-filter": idx_filter,
        "scan-filter": scan_filter,
    }[query]


# ---------------------------------------------------------------------------
# Queries (must match tiered-bench.rs)
# ---------------------------------------------------------------------------
QUERIES = {
    "post": """
        SELECT posts.id, posts.content, posts.created_at, posts.like_count,
               users.first_name, users.last_name, users.school, users.city
        FROM posts
        JOIN users ON users.id = posts.user_id
        WHERE posts.id = ?1
    """,
    "profile": """
        SELECT users.first_name, users.last_name, users.school, users.city, users.bio,
               posts.id, posts.content, posts.created_at, posts.like_count
        FROM users
        JOIN posts ON posts.user_id = users.id
        WHERE users.id = ?1
        ORDER BY posts.created_at DESC
        LIMIT 10
    """,
    "who-liked": """
        SELECT users.first_name, users.last_name, users.school, likes.created_at
        FROM likes
        JOIN users ON users.id = likes.user_id
        WHERE likes.post_id = ?1
        ORDER BY likes.created_at DESC
        LIMIT 50
    """,
    "mutual": """
        SELECT users.id, users.first_name, users.last_name, users.school
        FROM friendships
        JOIN friendships AS friendships_b ON friendships.user_b = friendships_b.user_b
        JOIN users ON users.id = friendships.user_b
        WHERE friendships.user_a = ?1 AND friendships_b.user_a = ?2
        LIMIT 20
    """,
    "idx-filter": """
        SELECT COUNT(*) FROM posts WHERE user_id = ?1
    """,
    "scan-filter": """
        SELECT COUNT(*) FROM posts WHERE like_count > ?1
    """,
}


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def get_bucket():
    bucket_name = os.environ.get("TIERED_TEST_BUCKET") or os.environ.get("BUCKET_NAME")
    if not bucket_name:
        raise RuntimeError("Set TIERED_TEST_BUCKET or BUCKET_NAME")

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_REGION", "auto")

    session = boto3.Session()
    client_kwargs = {"region_name": region}
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint

    s3 = session.resource("s3", **client_kwargs)
    return s3.Bucket(bucket_name)


def upload_if_missing(local_path: str, key_prefix: str, bucket, block_size: int):
    """Upload a plain SQLite file to sqlite-s3vfs format if the prefix is empty."""
    existing = list(bucket.objects.filter(Prefix=key_prefix + "/").limit(1))
    if existing:
        print(f"  [s3vfs] found existing prefix {key_prefix}/")
        return

    print(f"  [s3vfs] uploading {local_path} -> s3://{bucket.name}/{key_prefix}/")
    vfs = sqlite_s3vfs.S3VFS(bucket=bucket, block_size=block_size)
    with open(local_path, "rb") as f:
        vfs.deserialize_iter(key_prefix=key_prefix, bytes_iter=iter(lambda: f.read(1024 * 1024), b""))
    print(f"  [s3vfs] upload complete")


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------
def run_query_cold(
    bucket,
    key_prefix: str,
    block_size: int,
    sql: str,
    params: tuple,
    per_query_timeout: float | None,
) -> dict:
    """Run a single query with a fresh S3VFS + connection (cold)."""
    get_count = 0
    bytes_read = 0

    # Monkey-patch sqlite_s3vfs's page fetch to count S3 GetObject calls and
    # bytes. Patching boto3's resource Object class does not work because
    # sqlite_s3vfs keeps a different bucket resource with its own Object class.
    orig_block_bytes = sqlite_s3vfs.S3VFSFile._block_bytes

    def counted_block_bytes(self, block):
        nonlocal get_count, bytes_read
        block_bytes = orig_block_bytes(self, block)
        get_count += 1
        bytes_read += len(block_bytes)
        return block_bytes

    sqlite_s3vfs.S3VFSFile._block_bytes = counted_block_bytes
    try:
        vfs = sqlite_s3vfs.S3VFS(bucket=bucket, block_size=block_size)
        start = time.perf_counter()
        with apsw.Connection(key_prefix, vfs=vfs.name) as conn:
            cursor = conn.cursor()
            # Ensure page-size matches block size (database header already has it,
            # but this prevents accidental mismatches).
            cursor.execute("PRAGMA page_size", ())
            actual_ps = cursor.fetchall()[0][0]
            if actual_ps != block_size:
                raise RuntimeError(
                    f"SQLite page_size {actual_ps} != s3vfs block_size {block_size}"
                )

            if per_query_timeout:
                # APSW supports busy timeout but not statement timeout directly.
                # We use Python's signal-based timeout as a safety net.
                import signal

                def _timeout_handler(signum, frame):
                    raise TimeoutError(f"query exceeded {per_query_timeout}s")

                signal.signal(signal.SIGALRM, _timeout_handler)
                signal.setitimer(signal.ITIMER_REAL, per_query_timeout)
                try:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                finally:
                    signal.alarm(0)
            else:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        elapsed = time.perf_counter() - start
        return {
            "elapsed_ms": elapsed * 1000.0,
            "gets": get_count,
            "bytes": bytes_read,
            "rows": len(rows),
        }
    finally:
        sqlite_s3vfs.S3VFSFile._block_bytes = orig_block_bytes


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def bench_query(
    bucket,
    key_prefix: str,
    block_size: int,
    query: str,
    n_posts: int,
    n_users: int,
    iterations: int,
    warmup: int,
    per_query_timeout: float | None,
) -> dict:
    param_fn = make_param_fn(n_posts, n_users, query)
    sql = QUERIES[query]

    # Warmup iterations (not measured)
    for i in range(warmup):
        params = param_fn(i)
        run_query_cold(bucket, key_prefix, block_size, sql, params, per_query_timeout)

    times: List[float] = []
    gets: List[int] = []
    bytes_list: List[int] = []

    for i in range(warmup, warmup + iterations):
        params = param_fn(i)
        result = run_query_cold(
            bucket, key_prefix, block_size, sql, params, per_query_timeout
        )
        times.append(result["elapsed_ms"])
        gets.append(result["gets"])
        bytes_list.append(result["bytes"])

    times.sort()
    p50 = percentile(times, 50)
    p90 = percentile(times, 90)
    p99 = percentile(times, 99)
    avg_gets = statistics.mean(gets)
    avg_bytes = statistics.mean(bytes_list)

    return {
        "query": query,
        "p50": p50,
        "p90": p90,
        "p99": p99,
        "avg_gets": avg_gets,
        "avg_bytes_mb": avg_bytes / (1024.0 * 1024.0),
    }


def main():
    parser = argparse.ArgumentParser(description="Benchmark sqlite-s3vfs cold latency")
    parser.add_argument("--sizes", type=int, default=100000, help="Number of posts")
    parser.add_argument(
        "--local-db",
        default=None,
        help="Path to local SQLite file (default: /data/social_<size>.db)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=65536,
        help="SQLite page size / s3vfs block size",
    )
    parser.add_argument(
        "--iterations", type=int, default=10, help="Measured iterations per query"
    )
    parser.add_argument(
        "--warmup", type=int, default=2, help="Warmup iterations before measuring"
    )
    parser.add_argument(
        "--queries",
        default="post,profile,who-liked,mutual,idx-filter,scan-filter",
        help="Comma-separated query names",
    )
    parser.add_argument(
        "--per-query-timeout",
        type=float,
        default=120.0,
        help="Abort a single query iteration after this many seconds",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Assume the sqlite-s3vfs dataset already exists; do not upload",
    )
    args = parser.parse_args()

    n_posts = args.sizes
    n_users = max(n_posts // 10, 100)
    local_db = args.local_db or f"/data/social_{n_posts}.db"
    key_prefix = f"s3vfs_social_{n_posts}"
    block_size = args.page_size
    queries = [q.strip() for q in args.queries.split(",")]

    if not os.path.exists(local_db):
        raise FileNotFoundError(f"Local SQLite file not found: {local_db}")

    bucket = get_bucket()

    if not args.skip_upload:
        upload_if_missing(local_db, key_prefix, bucket, block_size)

    print()
    print("=== sqlite-s3vfs cold benchmark ===")
    print(f"  dataset: {n_posts} posts / {n_users} users")
    print(f"  local:   {local_db}")
    print(f"  s3 prefix: s3://{bucket.name}/{key_prefix}/")
    print(f"  page/block size: {block_size} bytes")
    print(f"  iterations: {args.iterations}, warmup: {args.warmup}")
    print()
    print("                                  p50        p90        p99    s3 GETs     s3 bytes")
    print("  ------------------------ ---------- ---------- ---------- ---------- ------------")

    for query in queries:
        try:
            result = bench_query(
                bucket,
                key_prefix,
                block_size,
                query,
                n_posts,
                n_users,
                args.iterations,
                args.warmup,
                args.per_query_timeout,
            )
            print(
                f"  [s3vfs] {query:18} {result['p50']:8.1f}ms {result['p90']:8.1f}ms "
                f"{result['p99']:8.1f}ms {result['avg_gets']:10.1f} {result['avg_bytes_mb']:10.1f}MB"
            )
        except TimeoutError as e:
            print(f"  [s3vfs] {query:18} TIMEOUT ({e})")
        except Exception as e:
            print(f"  [s3vfs] {query:18} ERROR: {e}")


if __name__ == "__main__":
    main()
