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
| **Lookup** | No EQP info, point queries, DDL | `[0, 0.1, 0.2]` | First miss free, gentle ramp. Lookups hit 1-2 pages per tree. |

Each element in a schedule is the fraction of eligible sibling groups to prefetch on the Nth consecutive cache miss (per-tree). When misses exceed the array length, fraction=1.0 (all remaining siblings).

### Runtime tuning

Schedules can be changed per-connection without reopening via the `turbolite_config_set` SQL function:

```sql
-- Tune before a batch of queries
SELECT turbolite_config_set('prefetch_search', '0.4,0.3,0.3');
SELECT turbolite_config_set('prefetch_lookup', '0,0,0.2');
SELECT turbolite_config_set('prefetch', '0.5,0.5');     -- sets both
SELECT turbolite_config_set('prefetch_reset', '');        -- reset to defaults
SELECT turbolite_config_set('plan_aware', 'false');

-- Changes take effect on the next query (zero overhead when not used)
```

| Key | Values | Description |
|-----|--------|-------------|
| `prefetch` | Comma-separated floats | Convenience: sets both search and lookup schedules |
| `prefetch_search` | Comma-separated floats | SEARCH query prefetch schedule (aggressive) |
| `prefetch_lookup` | Comma-separated floats | Lookup/point query prefetch schedule (conservative) |
| `prefetch_reset` | Any (ignored) | Reset both schedules to defaults |
| `plan_aware` | `true`/`false` | Enable/disable SCAN bulk prefetch |

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
--prefetch-lookup  Lookup prefetch schedule (default: 0,0.1,0.2)
--prefetch-hops    Radial prefetch schedule for Positional strategy (default: 0.33,0.33)
--ppg              Pages per page group (default: 256)
--page-size        Page size in bytes (default: 65536)
--import           Import mode: "auto" generates locally then uploads, or path to existing .db
--skip-verify      Skip COUNT(*) verification
--cleanup          Delete S3 data after benchmark
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

### S3 Express One Zone (EC2 c5.2xlarge, us-east-2a, 8 dedicated vCPU, 16GB RAM, March 2026)

1M posts (1.46GB, 91 page groups):

| Query | none | interior | index | data |
|-------|------|----------|-------|------|
| Point lookup | 75ms | 11ms | 11ms | 157us |
| Profile (5 joins) | 202ms | 134ms | 113ms | 301us |
| Who-liked | 118ms | 51ms | 5ms | 259us |
| Mutual friends | 82ms | 25ms | 5ms | 116us |
| Indexed filter | 76ms | 12ms | 5ms | 106us |
| Full scan + filter | 691ms | 685ms | 562ms | 280ms |

### S3 Standard (EC2 c5.2xlarge, us-east-2a, 8 dedicated vCPU, 16GB RAM, March 2026)

1M posts (1.46GB, 91 page groups):

| Query | none | interior | index | data |
|-------|------|----------|-------|------|
| Point lookup | 156ms | 55ms | 54ms | 128us |
| Profile (5 joins) | 469ms | 342ms | 343ms | 295us |
| Who-liked | 275ms | 166ms | 31ms | 309us |
| Mutual friends | 162ms | 69ms | 28ms | 111us |
| Indexed filter | 164ms | 50ms | 23ms | 70us |
| Full scan + filter | 808ms | 722ms | 650ms | 285ms |

### Tigris (Fly.io iad, performance-8x, 8 dedicated vCPU, 16GB RAM, March 2026)

1M posts (1.46GB, 91 page groups):

| Query | none | interior | index | data |
|-------|------|----------|-------|------|
| Point lookup | 189ms | 31ms | 44ms | 110us |
| Profile (5 joins) | 433ms | 339ms | 244ms | 355us |
| Who-liked | 220ms | 112ms | 12ms | 374us |
| Mutual friends | 197ms | 42ms | 11ms | 179us |
| Indexed filter | 157ms | 71ms | 11ms | 157us |
| Full scan + filter | 984ms | 905ms | 712ms | 281ms |

### Comparison

S3 Express is the fastest option: single-digit ms GET latency from the same availability zone yields 75ms cold point lookups and 11ms at interior level. Regular S3 is roughly 2-3x slower than Express but still usable, with 156ms cold point lookups and 469ms cold profile queries. Tigris latency from Fly.io iad is comparable to standard S3 from EC2 in the same region (189ms vs 156ms cold point lookup, 433ms vs 469ms cold profile).

The "data" (fully cached) numbers are nearly identical across all three environments (110-157us for point lookups), confirming that the difference between storage backends is purely storage latency, not CPU.

The gap between backends narrows at the "index" cache level because fewer S3 fetches are needed (only data pages). At "index" level, all three backends deliver single-digit to low-double-digit ms for targeted queries like who-liked and mutual friends.

For most workloads, the realistic operating state is "interior" (first query after connection open) or "index" (after background prefetch completes, which happens on first use and takes dozens to hundreds of milliseconds, depending on index size; this benchmark has 144MB index). Full scan is CPU-bound (all environments have 8 dedicated vCPU), so storage backend differences are smallest there.
