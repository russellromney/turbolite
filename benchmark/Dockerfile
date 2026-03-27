FROM lukemathwalker/cargo-chef:latest-rust-1-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
COPY bin/ bin/
COPY benchmark/ benchmark/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev cmake && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies only (cached unless Cargo.toml/Cargo.lock change)
RUN cargo chef cook --release --features tiered,zstd,bundled-sqlite --recipe-path recipe.json
# Copy source and build binary (only recompiles if source changed)
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
