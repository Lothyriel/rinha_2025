FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /recipe.json recipe.json
ENV RUSTFLAGS="-C target-cpu=skylake"
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release

FROM debian:stable-slim AS runtime
RUN apt-get update && apt-get install tini=0.19.0-1 -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
COPY --from=builder /target/release/rinha /

ENTRYPOINT ["/usr/bin/tini", "--", "./rinha"]
CMD []

EXPOSE 9999
