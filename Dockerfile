FROM rust:1.88 AS builder

COPY ./src ./src
COPY Cargo.toml ./

RUN cargo build --release

FROM alpine:latest

EXPOSE 3000

COPY --from=builder /target/release/rinha_2025 /

ENTRYPOINT ["./rinha_2025"]
