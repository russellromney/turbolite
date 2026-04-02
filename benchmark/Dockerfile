FROM rust:1-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev cmake && rm -rf /var/lib/apt/lists/*

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
RUN cargo build --release --features tiered,zstd,bundled-sqlite --bin tiered-bench --bin tiered-tune

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip && \
    apt-get remove -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/tiered-bench /usr/local/bin/tiered-bench
COPY --from=builder /app/target/release/tiered-tune /usr/local/bin/tiered-tune
COPY benchmark/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
