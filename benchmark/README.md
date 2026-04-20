# Tiered VFS Benchmark Guide

Core reference benchmark for the S3-backed page-group tiered VFS. Measures query latency against social network databases stored on S3-compatible object storage.

**Queries:** post+user join, profile (5 joins), who-liked, mutual friends, indexed filter, full scan + filter

**Cache levels:** none, interior, index, data (see below)

| Cache level | What's cached | What's fetched from S3 | When this happens |
|-------------|--------------|----------------------|-------------------|
| **none** | nothing | everything | Fresh start, empty cache |
| **interior** | interior B-tree pages | index + data pages | First query after connection open |
| **index** | interior + index pages | data pages only | Normal turbolite operation |
| **data** | everything | nothing | Equivalent to local SQLite |

## Dataset

Social network with 4 tables. At 1M posts: 100K users, 2.5M friendships, 3M likes (1.46GB, 91 page groups).

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT, last_name TEXT, email TEXT,
    school TEXT, city TEXT, bio TEXT, joined_at INTEGER
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER, content TEXT, created_at INTEGER, like_count INTEGER
);
CREATE TABLE friendships (
    user_a INTEGER, user_b INTEGER, created_at INTEGER,
    PRIMARY KEY (user_a, user_b)
);
CREATE TABLE likes (
    user_id INTEGER, post_id INTEGER, created_at INTEGER,
    PRIMARY KEY (user_id, post_id)
);

CREATE INDEX idx_posts_user ON posts(user_id);
CREATE INDEX idx_posts_created ON posts(created_at);
CREATE INDEX idx_friendships_b ON friendships(user_b, user_a);
CREATE INDEX idx_likes_post ON likes(post_id);
CREATE INDEX idx_likes_user ON likes(user_id, created_at);
CREATE INDEX idx_users_school ON users(school);
```

## Queries

| Name | SQL | What it tests |
|------|-----|---------------|
| **post** | `SELECT p.*, u.first_name, u.last_name, u.school, u.city FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ?` | Point lookup + 1 join. Touches 2 index pages + 2 data pages. |
| **profile** | `SELECT u.*, p.id, p.content, p.created_at, p.like_count FROM users u JOIN posts p ON p.user_id = u.id WHERE u.id = ? ORDER BY p.created_at DESC LIMIT 10` | User profile page. Index scan on posts(user_id), sorted by created_at. Multiple data pages. |
| **who-liked** | `SELECT u.first_name, u.last_name, u.school, l.created_at FROM likes l JOIN users u ON u.id = l.user_id WHERE l.post_id = ? ORDER BY l.created_at DESC LIMIT 50` | Reverse lookup through likes index, join to users. Up to 50 rows from scattered data pages. |
| **mutual** | `SELECT u.id, u.first_name, u.last_name, u.school FROM friendships f1 JOIN friendships f2 ON f1.user_b = f2.user_b JOIN users u ON u.id = f1.user_b WHERE f1.user_a = ? AND f2.user_a = ? LIMIT 20` | Self-join on friendships. Two index scans + intersection + user lookup. |
| **idx-filter** | `SELECT COUNT(*) FROM posts WHERE user_id = ?` | Indexed count. Covered by idx_posts_user, only index pages needed. |
| **scan-filter** | `SELECT COUNT(*) FROM posts WHERE like_count > ?` | Full table scan. No index on like_count, must read every data page. |

## Prefetch Tuning

The VFS uses three prefetch strategies, selected automatically per query via EXPLAIN QUERY PLAN:

| Strategy | When | Default schedule | What happens |
|----------|------|-----------------|--------------|
| **SCAN** (plan-aware) | EQP says `SCAN table` | All groups submitted upfront | Bulk prefetch of entire table before first read. No hop schedule involved. |
| **SEARCH** | EQP says `SEARCH table USING INDEX` | `[0.3, 0.3, 0.4]` | Aggressive warmup from first miss. SEARCH queries scan unknown portions of indexes/tables. |
| **Lookup** | No EQP info, point queries, DDL | `[0, 0, 0]` | Three free hops before any prefetch. Lookups hit 1-2 pages per tree. |

Each element in a schedule is the fraction of eligible sibling groups to prefetch on the Nth consecutive cache miss (per-tree). When misses exceed the array length, fraction=1.0 (all remaining siblings).

### Configuring schedules

Prefetch schedules are set on `TurboliteConfig` at VFS construction:

```rust
let config = TurboliteConfig {
    prefetch_search: vec![0.4, 0.3, 0.3],
    prefetch_lookup: vec![0.0, 0.0, 0.2],
    query_plan_prefetch: true, // enable plan-aware SCAN bulk prefetch
    ..Default::default()
};
```

| Field | Type | Description |
|-----|--------|-------------|
| `prefetch_search` | `Vec<f32>` | SEARCH query prefetch schedule (aggressive) |
| `prefetch_lookup` | `Vec<f32>` | Lookup/point query prefetch schedule (conservative) |
| `query_plan_prefetch` | `bool` | Enable/disable plan-aware SCAN bulk prefetch |

### Why two schedules?

SEARCH queries scan unknown portions of indexes/tables and need aggressive warmup. Lookups hit 1-2 pages per tree and barely need prefetch. Per-tree miss counters ensure independent tracking across trees in a query: a profile query hitting users (miss 1) then posts (miss 1) tracks each tree separately. SCAN queries need everything upfront and bypass schedules entirely (plan-aware bulk prefetch).

## CLI flags

```
--sizes            Row counts, comma-separated (default: 10000)
--iterations       Measured iterations per query per cache level (default: 10)
--warmup           Warmup iterations before measuring (default: 2)
--modes            Cache levels to run, comma-separated (default: all)
                   Options: none, interior, index, data
--queries          Queries to run, comma-separated (default: all)
                   Options: post, profile, who-liked, mutual, idx-filter, scan-filter
--prefetch-threads Worker threads for parallel S3 fetches (default: 8)
--prefetch-search  SEARCH prefetch schedule (default: 0.3,0.3,0.4)
--prefetch-lookup  Lookup prefetch schedule (default: 0,0,0)
--prefetch-hops    Radial prefetch schedule for Positional strategy (default: 0.33,0.33)
--ppg              Pages per page group (default: 256)
--page-size        Page size in bytes (default: 65536)
--import           Import mode: "auto" generates locally then uploads, or path to existing .db
--skip-verify      Skip COUNT(*) verification
--cleanup          Delete S3 data after benchmark
--plan-aware       Enable Phase Marne query-plan-aware prefetch
--post-prefetch    Per-query SEARCH schedule for post+user (default: off)
--post-lookup      Per-query LOOKUP schedule for post+user (default: off)
--profile-prefetch Per-query SEARCH schedule for profile (default: 0.1,0.2,0.3)
--profile-lookup   Per-query LOOKUP schedule for profile (default: 0,0,0,0)
--who-liked-prefetch  SEARCH schedule for who-liked (default: 0.3,0.3,0.4)
--who-liked-lookup    LOOKUP schedule for who-liked (default: 0,0,0)
--mutual-prefetch  SEARCH schedule for mutual (default: 0.4,0.3,0.3)
--mutual-lookup    LOOKUP schedule for mutual (default: 0,0,0)
--idx-filter-prefetch  SEARCH schedule for idx-filter (default: 0.2,0.3,0.5)
--idx-filter-lookup    LOOKUP schedule for idx-filter (default: 0,0,0)
--scan-filter-prefetch SEARCH schedule for scan-filter (default: off)
--scan-filter-lookup   LOOKUP schedule for scan-filter (default: off)
--matrix           Sweep schedule pairs at "none" level (see Matrix Mode below)
--matrix-schedules Schedule pairs to test (semicolon-separated, "search/lookup" format)
```

## Matrix Mode

Matrix mode (`--matrix`) tests each query at cache level "none" with every schedule pair in `--matrix-schedules`. This produces a comparison table per query showing how different search/lookup schedule combinations affect latency, GET count, and bytes transferred.

```bash
# Default 10 schedule pairs (off, old uniform, current dual, aggressive, etc.)
cargo run --release --features tiered,zstd --bin tiered-bench -- \
    --sizes 1000000 --import auto --plan-aware --matrix --iterations 10

# Custom schedule pairs
cargo run --release --features tiered,zstd --bin tiered-bench -- \
    --sizes 1000000 --plan-aware --matrix --iterations 10 \
    --matrix-schedules "off;0.3,0.3,0.4/0,0.1,0.2;0.5,0.5/0,0,0.1;1.0/0"
```

Schedule pair format: `search_schedule/lookup_schedule`, semicolon-separated. `off` = no prefetch (both zeros). No slash = same schedule for both.

Output is a table per query:

```
  --- post+user ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                                96.0ms    111.7ms       13.9       16.2MB
  0.30,0.30,0.40 / 0.00,0.10,0.20          77.1ms     94.7ms       14.0       17.1MB
  0.50,0.50 / 0.00,0.00,0.10               74.2ms     99.1ms       11.5        6.8MB
```

## Tuning Tool (tiered-tune)

`tiered-tune` connects to an existing turbolite database and sweeps prefetch schedules against your actual queries. Instead of benchmarking a synthetic dataset, tune against your real workload.

```bash
# Tune against an existing database
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://t3.storage.dev \
  cargo run --release --features tiered,zstd --bin tiered-tune -- \
    --prefix "databases/tenant-123" \
    --query "SELECT * FROM users WHERE id = ?1" --param 42 \
    --query "SELECT p.*, u.name FROM posts p JOIN users u ON p.user_id = u.id WHERE p.id = ?1" --param 100 \
    --plan-aware --iterations 10

# Custom schedule grid
cargo run --release --features tiered,zstd --bin tiered-tune -- \
    --prefix "databases/tenant-123" \
    --query "SELECT * FROM orders WHERE user_id = ?1 ORDER BY created_at DESC LIMIT 20" --param 1 \
    --search-schedules "0.3,0.3,0.4;0.5,0.5;1.0" \
    --lookup-schedules "0;0,0,0.1;0,0,0,0.1,0.2" \
    --plan-aware --iterations 10
```

The tool builds a Cartesian product of search x lookup schedules, tests each pair, and recommends the best one with the Rust config to apply it:

```
  Best: 0.50,0.50 / 0.00,0.00,0.10 (p50 = 74.2ms)
  TurboliteConfig.prefetch_search = vec![0.5, 0.5];
  TurboliteConfig.prefetch_lookup = vec![0.0, 0.0, 0.1];
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TIERED_TEST_BUCKET` | yes | -- | S3 bucket name (also accepts `BUCKET_NAME`) |
| `AWS_REGION` | no | from env | AWS region |
| `AWS_ENDPOINT_URL` | no | default S3 | Custom endpoint (Tigris: `https://fly.storage.tigris.dev`) |

## 1. Local (against Tigris)

Set `TIERED_TEST_BUCKET`, `AWS_REGION`, `AWS_ENDPOINT_URL`, and AWS credentials as environment variables, then run:

```bash
# Generate 100K rows, upload to S3, run all cache levels
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
  cargo run --release --features tiered,zstd --bin tiered-bench -- \
    --sizes 100000 --import auto

# Reuse existing 1M-row data on S3
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
  cargo run --release --features tiered,zstd --bin tiered-bench -- \
    --sizes 1000000 --modes interior,index

# Quick: just point lookups at interior level
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
  cargo run --release --features tiered,zstd --bin tiered-bench -- \
    --sizes 1000000 --modes interior --queries post
```

Data is stored at `social_{size}/` in the bucket and reused across runs automatically. No need for `--import` after the first run.

## 2. Fly.io (against Tigris)

The Fly app `turbolite-tiered-bench` is pre-configured in `fly.toml`:
- Region: `iad`
- VM: `performance-8x` (8 dedicated vCPU, 16GB RAM)
- Volume: 40GB at `/data`
- S3 credentials set via `fly secrets`

### Deploy and run

```bash
# From project root (not benchmark/)
fly deploy --app turbolite-tiered-bench \
  --config benchmark/fly.toml \
  --dockerfile benchmark/Dockerfile \
  --remote-only

# Machine starts automatically, runs the benchmark, then stops.
# If it doesn't start, kick it manually:
fly machine start <machine-id> --app turbolite-tiered-bench
```

The process args in `fly.toml` control what runs:

```toml
[processes]
  app = "--sizes 1000000 --iterations 10 --skip-verify --modes none,interior,index,data"
```

### View results

Results are uploaded to S3 by the entrypoint:

```bash
aws s3 ls s3://$TIERED_TEST_BUCKET/bench/logs/ --endpoint-url $AWS_ENDPOINT_URL

# Download latest
aws s3 cp s3://$TIERED_TEST_BUCKET/bench/logs/<filename>.log /tmp/bench.log \
  --endpoint-url $AWS_ENDPOINT_URL

# Parse results
grep -E '\[data\]|\[index\]|\[interior\]|\[none\]' /tmp/bench.log
```

Or check Fly logs directly:

```bash
fly logs --app turbolite-tiered-bench --no-tail | grep -E '\[data\]|\[index\]|\[interior\]|\[none\]'
```

## 3. EC2 (against S3 Express or S3 Standard)

S3 Express One Zone provides single-digit ms latency from the same availability zone. Regular S3 Standard works too, with higher latency (~20-50ms GETs).

### Existing AWS Resources

| Resource | Value |
|----------|-------|
| S3 Express bucket | `turbolite-bench--use2-az1--x-s3` |
| S3 Standard bucket | `turbolite-bench-regular` |
| Region | `us-east-2` |
| AZ | `us-east-2a` (zone ID: `use2-az1`) |
| Security group | `sg-07069137083c088b7` (SSH open) |

### Build the binary

Cross-compiled binaries from cargo-lambda hang on Amazon Linux. Use Docker:

```bash
docker build --platform linux/amd64 -f - -o type=local,dest=./target/docker-out . <<'DOCKERFILE'
FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
COPY bin/ bin/
COPY benchmark/ benchmark/
RUN cargo build --release --features tiered,zstd --bin tiered-bench

FROM scratch
COPY --from=builder /app/target/release/tiered-bench /tiered-bench
DOCKERFILE
```

### Launch, upload, run

```bash
AMI=$(aws ec2 describe-images --region us-east-2 --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
  --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text)

INSTANCE_ID=$(aws ec2 run-instances --region us-east-2 \
  --image-id $AMI --instance-type t3.medium --key-name russellromney \
  --security-group-ids sg-07069137083c088b7 \
  --placement AvailabilityZone=us-east-2a \
  --iam-instance-profile Name=turbolite-bench-ec2 \
  --associate-public-ip-address \
  --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --region us-east-2 --instance-ids $INSTANCE_ID
IP=$(aws ec2 describe-instances --region us-east-2 --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

# Push SSH key (expires in 60s), then upload binary
aws ec2-instance-connect send-ssh-public-key --region us-east-2 \
  --instance-id $INSTANCE_ID --instance-os-user ec2-user \
  --ssh-public-key file://~/.ssh/id_rsa.pub

scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa \
  ./target/docker-out/tiered-bench ec2-user@$IP:/home/ec2-user/tiered-bench

# Push key again, then run
aws ec2-instance-connect send-ssh-public-key --region us-east-2 \
  --instance-id $INSTANCE_ID --instance-os-user ec2-user \
  --ssh-public-key file://~/.ssh/id_rsa.pub

ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=15 -i ~/.ssh/id_rsa ec2-user@$IP \
  'chmod +x /home/ec2-user/tiered-bench && \
   BUCKET_NAME=turbolite-bench--use2-az1--x-s3 AWS_REGION=us-east-2 \
   /home/ec2-user/tiered-bench --sizes 100000 --import auto 2>&1'

# Or use regular S3 instead of S3 Express:
#  BUCKET_NAME=turbolite-bench-regular AWS_REGION=us-east-2 \
#  /home/ec2-user/tiered-bench --sizes 100000 --import auto 2>&1'

# Terminate when done
aws ec2 terminate-instances --region us-east-2 --instance-ids $INSTANCE_ID
```

## Recreating the IAM Instance Profile (if deleted)

```bash
aws iam create-role --role-name turbolite-bench-ec2-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam put-role-policy --role-name turbolite-bench-ec2-role --policy-name s3-express-access \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3express:CreateSession","s3:GetObject","s3:PutObject","s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3express:us-east-2:462570052286:bucket/turbolite-bench--use2-az1--x-s3","arn:aws:s3express:us-east-2:462570052286:bucket/turbolite-bench--use2-az1--x-s3/*","arn:aws:s3:::turbolite-bench-regular","arn:aws:s3:::turbolite-bench-regular/*"]}]}'

aws iam create-instance-profile --instance-profile-name turbolite-bench-ec2
aws iam add-role-to-instance-profile --instance-profile-name turbolite-bench-ec2 --role-name turbolite-bench-ec2-role
sleep 10
```

## Gotchas

- **cargo-lambda binaries hang on AWS Linux.** Always use the Docker build for EC2 deployments.
- **EC2 Instance Connect keys expire in 60 seconds.** Push a new key before each SSH/SCP command.
- **Fly deploy from project root.** Run `fly deploy --config benchmark/fly.toml --dockerfile benchmark/Dockerfile` from the project root, not from `benchmark/`.
- **Machine may not auto-start after deploy.** Use `fly machine start <id>` if the machine stops immediately after deploy.
- **Data reuse is automatic.** Data lives at `social_{size}/` in the bucket. The bench checks for an existing manifest before generating.

## Reference Results

### Default schedules (cache level: none, 10 iterations, March 2026)

1M posts (812MB, 51 page groups, BTreeAware grouping, 64KB pages, 256 ppg).
Default prefetch: search `[0.3, 0.3, 0.4]`, lookup `[0, 0, 0]`, plan-aware enabled.
Per-query schedules: search and lookup set independently per query (see CLI flags above).

**S3 Express One Zone** (EC2 c5.2xlarge, us-east-2a, ~4ms GET latency):

| Query | p50 | p90 | Avg GETs | Avg bytes |
|-------|-----|-----|----------|-----------|
| post+user | 77ms | 93ms | 10.5 | 4MB |
| profile | 190ms | 278ms | 41.7 | 54MB |
| who-liked | 129ms | 182ms | 15.7 | 14MB |
| mutual | 82ms | 114ms | 17.3 | 29MB |
| idx-filter | 74ms | 86ms | 8.0 | 3MB |
| scan-filter | 586ms | 692ms | 98.4 | 139MB |

**Tigris** (Fly.io iad, performance-8x, ~25ms GET latency):

Per-query schedules tuned for Tigris latency (warmer lookups than S3 Express defaults).
Best p50 from matrix sweep of 17 search/lookup schedule pairs per query.

| Query | Best schedule | p50 | p90 | Avg GETs | Avg bytes |
|-------|--------------|-----|-----|----------|-----------|
| post+user | `1.00 / 0,0,0.10` | 192ms | 324ms | 20.0 | 31MB |
| profile | `0.20,0.30,0.50 / 0,0.10,0.20` | 524ms | 806ms | 41.5 | 54MB |
| who-liked | `0.30,0.30,0.40 / 0,0,0,0` | 340ms | 572ms | 16.6 | 24MB |
| mutual | `0.40,0.30,0.30 / 0.10,0.20,0.30` | 183ms | 264ms | 13.1 | 17MB |
| idx-filter | `0.30,0.30,0.40 / 0.10,0.20,0.30` | 173ms | 242ms | 19.5 | 38MB |
| scan-filter | `0.40,0.30,0.30 / 0,0,0` | 984ms | 1.3s | 102.4 | 139MB |

### Schedule matrix (cache level: none, 10 iterations, March 2026)

10 search/lookup schedule pairs tested per query. Key findings:

**S3 Express winners (by p50):**

| Query | Best schedule | p50 | vs off/off | GETs | Bytes |
|-------|--------------|-----|-----------|------|-------|
| post+user | `0.50,0.50 / 0,0,0.10` | 74ms | -23% | 11.5 | 7MB |
| profile | `0.30,0.30,0.40 / 0,0.10,0.20` | 188ms | -11% | 41.4 | 53MB |
| who-liked | `0.30,0.30,0.40 / 0,0.10,0.20` | 118ms | -16% | 15.7 | 15MB |
| mutual | `0.50,0.50 / 0.10,0.20,0.30` | 69ms | -11% | 11.8 | 13MB |
| idx-filter | `0.33,0.33,0.34 / 0.33,0.33,0.34` | 66ms | -5% | 12.4 | 17MB |
| scan-filter | `1.00 / 0` | 553ms | -5% | 97.9 | 139MB |

**Tigris winners (by p50):**

| Query | Best schedule | p50 | vs off/off | GETs | Bytes |
|-------|--------------|-----|-----------|------|-------|
| post+user | `1.00 / 0,0,0.10` | 192ms | -17% | 20.0 | 31MB |
| profile | `0.20,0.30,0.50 / 0,0.10,0.20` | 524ms | -15% | 41.5 | 54MB |
| who-liked | `0.30,0.30,0.40 / 0,0,0,0` | 340ms | -20% | 16.6 | 24MB |
| mutual | `0.40,0.30,0.30 / 0.10,0.20,0.30` | 183ms | -34% | 13.1 | 17MB |
| idx-filter | `0.30,0.30,0.40 / 0.10,0.20,0.30` | 173ms | -8% | 19.5 | 38MB |
| scan-filter | `0.40,0.30,0.30 / 0,0,0` | 984ms | -18% | 102.4 | 139MB |

### Key observations

**Storage backend determines optimal tuning.** On S3 Express (~4ms GET), zero-heavy lookup schedules (`0,0,0`) dominate because individual range GETs cost only 4ms. On Tigris (~25ms GET), warmer lookup schedules (`0,0.1,0.2` or `0.1,0.2,0.3`) pay off because each avoided round trip saves 25ms. The per-query schedule system lets each query use the optimal pair for its backend.

**Search and lookup need independent tuning.** Setting both to the same schedule (the old behavior) accidentally worked on some queries but hurt others. Independent schedules let SEARCH queries use aggressive warmup while point-query lookups stay conservative. Profile went from 681ms to 480ms on Tigris once search and lookup were split.

**Scan-filter is schedule-insensitive.** Plan-aware frontrunning bulk-prefetches the entire table before the first read. Schedule variation is noise on both backends. This validates that frontrunning (Phase Marne) is doing its job for SCAN queries.

**off/off is a viable baseline.** With seekable sub-chunk encoding, each cache miss is a ~18KB range GET instead of a full 1.2MB group download. On S3 Express, off/off delivers 96ms point lookups and 212ms profiles. This validates that the sub-chunk architecture is sound on its own; prefetch is optimization on top, not a crutch.

### Full matrix data (S3 Express)

```
  --- post+user ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                                96.0ms    111.7ms       13.9       16.2MB
  0.33,0.33,0.34 / 0.33,0.33,0.34          95.3ms    118.1ms       16.5       11.0MB
  0.30,0.30,0.40 / 0.00,0.10,0.20          77.1ms     94.7ms       14.0       17.1MB
  0.30,0.30,0.40 / 0.30,0.30,0.40          92.0ms    113.1ms       16.2       10.3MB
  0.50,0.50 / 0.00,0.00,0.10               74.2ms     99.1ms       11.5        6.8MB
  0.50,0.50 / 0.10,0.20,0.30              102.3ms    128.5ms       22.7       28.3MB
  0.50,0.30,0.20 / 0.10,0.10,0.20          98.7ms    123.3ms       18.1       19.0MB
  1.00 / 0.00                              76.3ms     93.9ms       18.0       30.2MB
  0.20,0.30,0.50 / 0.00,0.00,0.20          87.6ms    132.4ms       16.0       20.3MB
  0.40,0.30,0.30 / 0.10,0.20,0.30          95.6ms    111.0ms       17.4       15.9MB

  --- profile ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               211.7ms    279.5ms       43.2       59.7MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         254.1ms    327.0ms       56.2       75.9MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         188.0ms    262.9ms       41.4       52.8MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         269.0ms    336.6ms       58.4       81.0MB
  0.50,0.50 / 0.00,0.00,0.10              218.5ms    295.8ms       46.1       67.0MB
  0.50,0.50 / 0.10,0.20,0.30              258.6ms    349.4ms       61.5       86.7MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         264.3ms    339.4ms       64.0       97.2MB
  1.00 / 0.00                             199.7ms    267.8ms       44.8       63.9MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         192.4ms    309.5ms       41.6       53.5MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         236.0ms    373.1ms       55.3       75.0MB

  --- who-liked ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               141.0ms    196.7ms       17.3       13.3MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         156.3ms    249.2ms       17.8       29.1MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         118.1ms    173.1ms       15.7       15.0MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         124.3ms    270.8ms       22.3       39.4MB
  0.50,0.50 / 0.00,0.00,0.10              130.8ms    254.7ms       22.6       34.9MB
  0.50,0.50 / 0.10,0.20,0.30              173.4ms    206.4ms       19.4       33.4MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         135.5ms    281.3ms       21.4       36.8MB
  1.00 / 0.00                             150.7ms    192.5ms       17.8       20.4MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         147.6ms    254.2ms       22.3       32.3MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         146.3ms    286.0ms       26.1       53.9MB

  --- mutual ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                                77.8ms     89.4ms       15.0       22.0MB
  0.33,0.33,0.34 / 0.33,0.33,0.34          74.0ms    145.4ms        8.8        4.0MB
  0.30,0.30,0.40 / 0.00,0.10,0.20          76.8ms     98.6ms       11.5        8.9MB
  0.30,0.30,0.40 / 0.30,0.30,0.40          73.0ms    110.1ms        8.4        2.8MB
  0.50,0.50 / 0.00,0.00,0.10               73.5ms    108.5ms        8.9        3.0MB
  0.50,0.50 / 0.10,0.20,0.30               69.4ms    132.4ms       11.8       13.3MB
  0.50,0.30,0.20 / 0.10,0.10,0.20          72.5ms    103.2ms        9.7        5.7MB
  1.00 / 0.00                              75.0ms     97.3ms       12.3       12.4MB
  0.20,0.30,0.50 / 0.00,0.00,0.20          78.4ms    100.4ms       13.5       15.8MB
  0.40,0.30,0.30 / 0.10,0.20,0.30          82.3ms     89.3ms        8.8        3.0MB

  --- idx-filter ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                                69.5ms     90.2ms        8.8        3.0MB
  0.33,0.33,0.34 / 0.33,0.33,0.34          65.5ms     81.4ms       12.4       17.2MB
  0.30,0.30,0.40 / 0.00,0.10,0.20          74.5ms    137.0ms       17.6       31.1MB
  0.30,0.30,0.40 / 0.30,0.30,0.40          76.1ms     92.1ms       11.5       10.9MB
  0.50,0.50 / 0.00,0.00,0.10               65.8ms     94.1ms       11.6       12.9MB
  0.50,0.50 / 0.10,0.20,0.30               69.7ms     96.2ms        8.2        2.8MB
  0.50,0.30,0.20 / 0.10,0.10,0.20          71.2ms    109.5ms       14.4       22.3MB
  1.00 / 0.00                              81.8ms    103.5ms       16.0       26.0MB
  0.20,0.30,0.50 / 0.00,0.00,0.20          77.1ms    100.5ms       21.0       42.2MB
  0.40,0.30,0.30 / 0.10,0.20,0.30          69.4ms     81.2ms       12.1       16.1MB

  --- scan-filter ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               580.6ms    646.0ms       98.2      138.6MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         579.0ms    663.1ms      102.7      152.6MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         591.0ms    673.3ms      102.3      152.6MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         574.5ms    666.8ms       97.8      138.6MB
  0.50,0.50 / 0.00,0.00,0.10              577.3ms    598.8ms       97.8      138.6MB
  0.50,0.50 / 0.10,0.20,0.30              566.7ms    701.6ms       98.4      138.6MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         574.0ms    635.6ms       97.3      138.5MB
  1.00 / 0.00                             553.2ms    623.4ms       97.9      138.6MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         554.9ms    632.3ms       97.5      138.5MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         554.1ms    637.5ms       97.2      138.5MB
```

### Full matrix data (Tigris)

```
  --- post+user ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               243.8ms    348.1ms       21.3       42.6MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         245.0ms    344.2ms       18.5       21.2MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         259.2ms    394.8ms       23.6       46.0MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         238.3ms    411.2ms       16.5       13.1MB
  0.50,0.50 / 0.00,0.00,0.10              289.8ms    363.1ms       18.9       31.9MB
  0.50,0.50 / 0.10,0.20,0.30              281.7ms    440.6ms       20.1       26.0MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         218.6ms    271.0ms       23.7       37.4MB
  1.00 / 0.00                             260.0ms    500.8ms       19.5       37.3MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         231.6ms    286.2ms       18.9       30.8MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         261.3ms    447.1ms       23.6       37.1MB

  --- profile ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               727.0ms       1.1s       54.5       93.5MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         730.7ms    982.4ms       82.1      127.2MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         680.6ms    824.5ms       47.5       74.9MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         667.0ms    997.2ms       90.8      137.5MB
  0.50,0.50 / 0.00,0.00,0.10              549.0ms    920.5ms       40.8       50.0MB
  0.50,0.50 / 0.10,0.20,0.30              700.6ms    824.9ms       76.7      113.8MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         609.4ms    937.8ms       75.6      111.0MB
  1.00 / 0.00                             688.8ms       1.1s       54.8       95.4MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         634.5ms    675.9ms       50.4       80.8MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         705.9ms    934.0ms       84.8      130.4MB

  --- who-liked ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               424.3ms    523.3ms       24.6       48.6MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         413.6ms    750.0ms       21.8       49.1MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         384.1ms    482.8ms       31.1       73.4MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         417.5ms    579.5ms       26.9       68.4MB
  0.50,0.50 / 0.00,0.00,0.10              471.9ms    550.5ms       30.5       77.0MB
  0.50,0.50 / 0.10,0.20,0.30              380.1ms    612.4ms       21.3       51.3MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         488.0ms    618.1ms       31.0       87.7MB
  1.00 / 0.00                             533.0ms    720.6ms       30.5       79.3MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         374.0ms    595.2ms       26.6       61.3MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         369.0ms    451.8ms       31.2       82.0MB

  --- mutual ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               209.9ms    315.7ms       16.2       29.4MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         208.8ms    256.1ms       15.9       27.4MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         200.5ms    425.7ms        9.1        4.3MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         188.0ms    369.1ms       20.5       43.6MB
  0.50,0.50 / 0.00,0.00,0.10              235.2ms    305.7ms       15.9       25.4MB
  0.50,0.50 / 0.10,0.20,0.30              214.1ms    329.9ms       17.5       29.0MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         204.3ms    340.7ms       10.3       10.2MB
  1.00 / 0.00                             231.5ms    329.6ms        8.7        3.8MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         162.0ms    225.9ms       16.2       27.9MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         156.8ms    227.9ms       17.4       31.2MB

  --- idx-filter ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               259.8ms    472.6ms       16.5       30.6MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         198.3ms    446.9ms       17.1       30.8MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         158.9ms    257.4ms       18.4       32.6MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         344.2ms    535.8ms       16.6       32.6MB
  0.50,0.50 / 0.00,0.00,0.10              176.2ms    232.1ms       16.7       30.7MB
  0.50,0.50 / 0.10,0.20,0.30              215.5ms    343.5ms       18.9       38.7MB
  0.50,0.30,0.20 / 0.10,0.10,0.20         245.5ms    392.8ms       16.8       30.0MB
  1.00 / 0.00                             185.8ms    311.5ms       19.8       40.3MB
  0.20,0.30,0.50 / 0.00,0.00,0.20         234.6ms    366.0ms       14.4       22.7MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         163.5ms    359.3ms       12.2       16.6MB

  --- scan-filter ---
  search / lookup                             p50        p90       GETs        bytes
  ------------------------------------ ---------- ---------- ---------- ------------
  off / off                               922.3ms       1.2s      108.0      152.7MB
  0.33,0.33,0.34 / 0.33,0.33,0.34         900.7ms       1.1s      107.5      152.7MB
  0.30,0.30,0.40 / 0.00,0.10,0.20         920.5ms       1.0s      105.4      152.7MB
  0.30,0.30,0.40 / 0.30,0.30,0.40         973.5ms       1.1s      101.8      138.6MB
  0.50,0.50 / 0.00,0.00,0.10              944.0ms       1.2s      107.2      152.7MB
  0.50,0.50 / 0.10,0.20,0.30              937.3ms       1.6s      108.8      152.8MB
  0.50,0.30,0.20 / 0.10,0.10,0.20            1.1s       1.2s      108.9      152.8MB
  1.00 / 0.00                                1.1s       1.3s      109.8      152.8MB
  0.20,0.30,0.50 / 0.00,0.00,0.20            1.0s       1.4s      109.3      152.8MB
  0.40,0.30,0.30 / 0.10,0.20,0.30         952.8ms       1.2s      107.3      138.7MB
```
