FROM rust:1-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev cmake libclang-dev && rm -rf /var/lib/apt/lists/*

# Stub out path dependencies that aren't needed for the benchmark.
# walrust-core and hadb-io are behind the "wal" feature flag which
# the benchmark doesn't use. But cargo resolves path deps at parse time
# even for unused optional deps, so we create stub crates.
RUN mkdir -p /walrust/crates/walrust-core/src && \
    echo '[package]\nname = "walrust"\nversion = "0.3.0"\nedition = "2021"' > /walrust/crates/walrust-core/Cargo.toml && \
    echo '' > /walrust/crates/walrust-core/src/lib.rs && \
    mkdir -p /hadb/hadb-io/src && \
    echo '[package]\nname = "hadb-io"\nversion = "0.2.0"\nedition = "2021"' > /hadb/hadb-io/Cargo.toml && \
    echo '' > /hadb/hadb-io/src/lib.rs

COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
COPY bin/ bin/
COPY benchmark/ benchmark/
# turbolite-ffi is a workspace member; cargo needs its manifest to parse the
# workspace even though we only build the root crate's bench binaries.
COPY turbolite-ffi/ turbolite-ffi/
RUN cargo build --release --features bench-s3,zstd,bundled-sqlite --bin tiered-bench --bin tiered-tune

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y ca-certificates curl unzip python3 python3-pip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip && \
    apt-get remove -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*
RUN pip3 install --break-system-packages apsw boto3 sqlite-s3vfs sqlite-graft s3sqlite
COPY --from=builder /app/target/release/tiered-bench /usr/local/bin/tiered-bench
COPY --from=builder /app/target/release/tiered-tune /usr/local/bin/tiered-tune
COPY benchmark/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY benchmark/bench_s3vfs.py /usr/local/bin/bench_s3vfs.py
COPY benchmark/bench_graft.py /usr/local/bin/bench_graft.py
COPY benchmark/bench_s3sqlite.py /usr/local/bin/bench_s3sqlite.py
COPY benchmark/generate_social_db.py /usr/local/bin/generate_social_db.py
COPY benchmark/run_s3vfs_bench.sh /usr/local/bin/run_s3vfs_bench.sh
COPY benchmark/run_graft_bench.sh /usr/local/bin/run_graft_bench.sh
COPY benchmark/run_s3sqlite_bench.sh /usr/local/bin/run_s3sqlite_bench.sh
RUN chmod +x /usr/local/bin/entrypoint.sh \
    /usr/local/bin/bench_s3vfs.py \
    /usr/local/bin/bench_graft.py \
    /usr/local/bin/bench_s3sqlite.py \
    /usr/local/bin/generate_social_db.py \
    /usr/local/bin/run_s3vfs_bench.sh \
    /usr/local/bin/run_graft_bench.sh \
    /usr/local/bin/run_s3sqlite_bench.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
