#!/usr/bin/env python3
"""Benchmark Graft (libgraft) against the same social queries as tiered-bench.

Graft docs: https://graft.rs/docs/sqlite/
Import: VACUUM INTO 'file:<tag>?vfs=graft'
Query:  sqlite3.connect('file:<tag>?vfs=graft', uri=True)

Cold means a fresh local data_dir per iteration, cloning from the remote Log ID
that was produced by the one-time import+push.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Same queries as bench_s3vfs.py / tiered-bench
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


def load_graft(conn: sqlite3.Connection) -> None:
    import sqlite_graft

    conn.enable_load_extension(True)
    sqlite_graft.load(conn)


def write_config(data_dir: str) -> str:
    bucket = os.environ["TIERED_TEST_BUCKET"]
    endpoint = os.environ.get("AWS_ENDPOINT", os.environ.get("AWS_ENDPOINT_URL", ""))
    # Graft reads AWS_ENDPOINT for S3-compatible endpoint.
    # Make sure it is set for the import/push process too.
    if endpoint and "AWS_ENDPOINT" not in os.environ:
        os.environ["AWS_ENDPOINT"] = endpoint
    path = Path(data_dir) / "graft.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'data_dir = "{data_dir}/graft_data"\n'
        f'\n'
        f'[remote]\n'
        f'type = "s3_compatible"\n'
        f'bucket = "{bucket}"\n'
        f'prefix = "graft_benchmark"\n'
    )
    return str(path)


def import_and_push(local_db: str, tag: str) -> str:
    """Import local_db into a Graft tag, push to S3, return remote log id."""
    data_dir = tempfile.mkdtemp(prefix="graft_import_")
    config_path = write_config(data_dir)
    # Graft reads its config from GRAFT_CONFIG at extension initialization time,
    # so it must be set in the process environment before load_graft().
    os.environ["GRAFT_CONFIG"] = config_path

    src = sqlite3.connect(local_db)
    load_graft(src)
    # Load extension and import. VACUUM INTO will write all pages through graft.
    src.execute(f"VACUUM INTO 'file:{tag}?vfs=graft'")
    src.close()

    # Open graft volume and push.
    conn = sqlite3.connect(f"file:{tag}?vfs=graft", uri=True)
    conn.execute("PRAGMA graft_push")
    conn.close()

    # Extract remote log id from graft_volumes output.
    conn = sqlite3.connect(f"file:{tag}?vfs=graft", uri=True)
    rows = conn.execute("PRAGMA graft_volumes").fetchall()
    conn.close()

    remote_log_id = None
    text = "\n".join(" ".join(str(c) for c in r) for r in rows)
    m = re.search(r"Remote:\s*([A-Za-z0-9_-]+)", text)
    if m:
        remote_log_id = m.group(1)

    shutil.rmtree(data_dir, ignore_errors=True)
    if not remote_log_id:
        raise RuntimeError(f"Could not parse remote log id from graft_volumes output:\n{text}")
    return remote_log_id


def run_query_cold(remote_log_id: str, tag: str, sql: str, params, timeout: int | None = None) -> dict:
    data_dir = tempfile.mkdtemp(prefix="graft_cold_")
    config_path = write_config(data_dir)
    env = os.environ.copy()
    env["GRAFT_CONFIG"] = config_path

    start = time.perf_counter()
    # Use a subprocess so we get a completely fresh Graft runtime for each iteration.
    # This avoids in-process caches and gives a true cold measurement.
    script = f'''
import os, sqlite3, sqlite_graft, sys
os.environ["GRAFT_CONFIG"] = {config_path!r}
conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
sqlite_graft.load(conn)
conn.close()

g = sqlite3.connect("file:{tag}?vfs=graft", uri=True)
g.execute("PRAGMA graft_clone = \\"{remote_log_id}\\"")
g.execute("PRAGMA graft_pull")
cur = g.execute({sql!r}, {params!r})
rows = cur.fetchall()
print("ROWS", len(rows))
g.close()
'''
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        elapsed = time.perf_counter() - start
        if proc.returncode != 0:
            return {"elapsed_ms": elapsed * 1000.0, "error": proc.stderr.strip() or proc.stdout.strip()}
        return {"elapsed_ms": elapsed * 1000.0, "rows": 0}
    except subprocess.TimeoutExpired:
        shutil.rmtree(data_dir, ignore_errors=True)
        return {"elapsed_ms": (time.perf_counter() - start) * 1000.0, "error": f"timeout after {timeout}s"}
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


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
    parser.add_argument("--tag", default="social_100000")
    parser.add_argument("--queries", default=",".join(QUERIES.keys()))
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--per-query-timeout", type=int, default=300)
    args = parser.parse_args()

    try:
        import sqlite_graft  # noqa: F401
    except ImportError:
        print("ERROR: sqlite_graft not installed. Run: pip install sqlite-graft", file=sys.stderr)
        return 1

    print(f"[graft] importing {args.local_db} into tag '{args.tag}' and pushing to S3...")
    remote_log_id = import_and_push(args.local_db, args.tag)
    print(f"[graft] remote log id: {remote_log_id}")

    query_names = [q.strip() for q in args.queries.split(",") if q.strip()]
    print(f"\n[graft] benchmark results (cold, fresh data_dir + subprocess per iteration)")
    print(f"{'query':16} {'p50':>10} {'p95':>10} {'p99':>10}")

    for name in query_names:
        if name not in QUERIES:
            print(f"[graft] unknown query: {name}", file=sys.stderr)
            continue
        sql, param_fn = QUERIES[name]
        times = []
        for i in range(args.warmup + args.iterations):
            params = param_fn()
            if not isinstance(params, tuple):
                params = (params,)
            result = run_query_cold(remote_log_id, args.tag, sql, params, args.per_query_timeout)
            if "error" in result:
                print(f"[graft] {name} error: {result['error']}", file=sys.stderr)
                break
            if i >= args.warmup:
                times.append(result["elapsed_ms"])
        if times:
            times.sort()
            print(f"  [graft] {name:13} {percentile(times, 50):>10.1f}ms {percentile(times, 95):>10.1f}ms {percentile(times, 99):>10.1f}ms")

    return 0


if __name__ == "__main__":
    sys.exit(main())
