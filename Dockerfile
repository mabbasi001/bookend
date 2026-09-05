# syntax=docker/dockerfile:1.7
#
# Two-stage build. Dependencies are compiled in their own layer via cargo-chef
# so a source-only change does not rebuild the whole dependency tree.

ARG RUST_VERSION=1
FROM rust:${RUST_VERSION}-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

# ---- plan: capture the dependency graph only ---------------------------------
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# ---- build -------------------------------------------------------------------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked \
 && strip target/release/bookend

# ---- runtime -----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: rustls-tls-native-roots needs the system CA bundle for exchange TLS.
# tzdata: not required (chrono uses UTC), kept out to stay small.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home /app --shell /usr/sbin/nologin mm

WORKDIR /app
COPY --from=builder /build/target/release/bookend /usr/local/bin/bookend
RUN mkdir -p /app/configs /app/data && chown -R mm:mm /app

USER mm

ENV RUST_LOG=info \
    RUST_BACKTRACE=1

# Configs are mounted read-only, state is written to /app/data (volume).
VOLUME ["/app/data"]

# Exec form: the binary is PID 1 and receives SIGTERM directly, so the
# cancel-all-orders shutdown sequence runs before the container stops.
ENTRYPOINT ["bookend"]
CMD ["--config", "/app/configs/paper.toml"]
