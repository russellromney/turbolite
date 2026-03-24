# Tiered VFS Benchmark Guide

Warm/cold benchmark for the S3-backed page-group tiered VFS. Measures query latency against social network databases stored on S3-compatible object storage. Default 64KB pages, 256 pages per group (16MB).

**Queries tested:** post+user join, user profile, who-liked, mutual friends
**Modes:** WARM (cache populated, pread only) and COLD (cache cleared, S3 fetch per iteration)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BUCKET_NAME` | yes | — | S3 bucket name |
| `AWS_REGION` | no | from env | AWS region |
| `AWS_ENDPOINT_URL` | no | default S3 | Custom endpoint (Tigris: `https://fly.storage.tigris.dev`) |
| `BENCH_SIZES` | no | `10000` | Row counts, comma-separated (e.g. `100000`) |
| `BENCH_REUSE` | no | — | Reuse existing S3 data at this prefix (skip data gen) |
| `BENCH_NO_CLEANUP` | no | false | Keep S3 data after benchmark |
| `BENCH_ITERATIONS` | no | `20` | Iterations per query per mode |
| `BENCH_PPG` | no | `256` | Pages per page group |
| `BENCH_PAGE_SIZE` | no | `65536` | Page size in bytes |

## 1. Local (against Tigris)

```bash
BUCKET_NAME=turbolite-test \
  AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
  BENCH_SIZES=100000 \
  BENCH_NO_CLEANUP=true \
  cargo run --release --features tiered,zstd --bin tiered-bench
```

To reuse previously uploaded data (skip data generation):

```bash
BUCKET_NAME=turbolite-test \
  AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
  BENCH_REUSE=social_100000 \
  BENCH_NO_CLEANUP=true \
  BENCH_SIZES=100000 \
  cargo run --release --features tiered,zstd --bin tiered-bench
```

AWS credentials come from Soup:

```bash
soup run -p turbolite -e development -- cargo run --release --features tiered,zstd --bin tiered-bench
```

## 2. Fly.io (against Tigris)

The Fly app `cinch-tiered-bench` is pre-configured in `fly.toml` with region `iad`, a mounted volume at `/data`, and a `performance-2x` VM with 4GB RAM.

### Deploy

```bash
fly deploy
```

### Run

```bash
fly ssh console -C "BUCKET_NAME=turbolite-test \
  AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
  BENCH_REUSE=social_100000 \
  BENCH_NO_CLEANUP=true \
  BENCH_SIZES=100000 \
  tiered-bench"
```

Or set env vars on the app and just run:

```bash
fly secrets set BUCKET_NAME=turbolite-test AWS_ENDPOINT_URL=https://fly.storage.tigris.dev
fly ssh console -C "BENCH_REUSE=social_100000 BENCH_NO_CLEANUP=true BENCH_SIZES=100000 tiered-bench"
```

## 3. EC2 (against S3 Express One Zone)

S3 Express provides single-digit ms latency from the same availability zone. This requires an EC2 instance in the same AZ as the S3 Express directory bucket.

### Existing AWS Resources

| Resource | Value |
|----------|-------|
| S3 Express bucket | `cinch-bench--use2-az1--x-s3` |
| Region | `us-east-2` |
| AZ | `us-east-2a` (zone ID: `use2-az1`) |
| Data prefix | `social_100000/` (11 page groups, ~28MB compressed) |
| Security group | `sg-07069137083c088b7` (SSH open) |

### Build the binary

**Do not use cargo-lambda** — its cross-compiled binaries hang on Amazon Linux. Use Docker:

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

Output: `./target/docker-out/tiered-bench`

### Launch EC2

```bash
# Find latest Amazon Linux 2023 AMI
AMI=$(aws ec2 describe-images --region us-east-2 --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
  --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text)

# Launch in us-east-2a (same AZ as S3 Express bucket)
INSTANCE_ID=$(aws ec2 run-instances --region us-east-2 \
  --image-id $AMI \
  --instance-type t3.medium \
  --key-name russellromney \
  --security-group-ids sg-07069137083c088b7 \
  --placement AvailabilityZone=us-east-2a \
  --iam-instance-profile Name=cinch-bench-ec2 \
  --associate-public-ip-address \
  --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"

# Wait for running state and get IP
aws ec2 wait instance-running --region us-east-2 --instance-ids $INSTANCE_ID
IP=$(aws ec2 describe-instances --region us-east-2 --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

echo "IP: $IP"
```

### Upload binary and run

EC2 Instance Connect pushes a temporary SSH key (expires in 60s):

```bash
# Push SSH key
aws ec2-instance-connect send-ssh-public-key --region us-east-2 \
  --instance-id $INSTANCE_ID \
  --instance-os-user ec2-user \
  --ssh-public-key file://~/.ssh/id_rsa.pub

# Upload binary (run within 60s of key push)
scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa \
  ./target/docker-out/tiered-bench ec2-user@$IP:/home/ec2-user/tiered-bench
```

Push key again before running:

```bash
aws ec2-instance-connect send-ssh-public-key --region us-east-2 \
  --instance-id $INSTANCE_ID \
  --instance-os-user ec2-user \
  --ssh-public-key file://~/.ssh/id_rsa.pub

ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=15 -i ~/.ssh/id_rsa ec2-user@$IP \
  'chmod +x /home/ec2-user/tiered-bench && \
   BUCKET_NAME=cinch-bench--use2-az1--x-s3 \
   AWS_REGION=us-east-2 \
   BENCH_REUSE=social_100000 \
   BENCH_NO_CLEANUP=true \
   BENCH_SIZES=100000 \
   /home/ec2-user/tiered-bench 2>&1'
```

### Terminate when done

```bash
aws ec2 terminate-instances --region us-east-2 --instance-ids $INSTANCE_ID
```

## 4. Recreating the IAM Instance Profile (if deleted)

```bash
# Create role with EC2 trust policy
aws iam create-role --role-name cinch-bench-ec2-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

# Add S3 Express permissions
aws iam put-role-policy --role-name cinch-bench-ec2-role --policy-name s3-express-access \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3express:CreateSession","s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3express:us-east-2:462570052286:bucket/cinch-bench--use2-az1--x-s3","arn:aws:s3express:us-east-2:462570052286:bucket/cinch-bench--use2-az1--x-s3/*"]}]}'

# Create instance profile and attach role
aws iam create-instance-profile --instance-profile-name cinch-bench-ec2
aws iam add-role-to-instance-profile --instance-profile-name cinch-bench-ec2 --role-name cinch-bench-ec2-role

# Wait 10s for IAM propagation before launching EC2
sleep 10
```

## Gotchas

- **cargo-lambda binaries hang on AWS Linux.** Always use the Docker build for EC2/Lambda deployments.
- **Manifest keys must match S3 keys exactly.** The `page_group_keys` array in `manifest.json` contains the full S3 object keys used by `GetObject`. If you copy data between buckets, verify the keys match.
- **EC2 Instance Connect keys expire in 60 seconds.** Push a new key before each SSH/SCP command.
- **Default VPC internet gateway may be detached.** If SSH times out, check: `aws ec2 describe-route-tables --region us-east-2` — the `0.0.0.0/0` route should point to an IGW (not show `blackhole`).

## Reference Results (64KB pages, March 2026, local Mac against Tigris)

### 100K posts (2,310 pages, 10 groups, 144MB)

| | warm p50 | cold p50 | arctic p50 |
|---|---|---|---|
| post+user | 63μs | 36ms (1 GET, 18KB) | 270ms |
| profile | 159μs | 457ms (11 GETs, 7.2MB) | 737ms |
| who-liked | 1.5ms | 43ms (1 GET, 18KB) | 813ms |
| mutual | 35μs | 51ms (1 GET, 18KB) | 495ms |

### 500K posts (11,569 pages, 46 groups, 723MB)

| | warm p50 | cold p50 | arctic p50 |
|---|---|---|---|
| post+user | 658μs | 116ms (2 GETs, 106KB) | 618ms |
| profile | 1.1ms | 893ms (31 GETs, 33MB) | 2.4s |
| who-liked | 1.5ms | 68ms (1 GET, 88KB) | 1.9s |
| mutual | 39μs | 60ms (1 GET, 88KB) | 834ms |

### Previous results (4KB pages, Fly.io + Tigris iad, 100K posts)

| | cold post+user p50 | cold mutual p50 | warm post+user p50 |
|---|---|---|---|
| **Fly.io + Tigris (iad)** | 121-211ms | 239-511ms | ~20μs |
| **EC2 + S3 Express (same AZ)** | 110.8ms | 228.9ms | 20μs |
| **Neon (reference)** | ~500ms | — | — |
