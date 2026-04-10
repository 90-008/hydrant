# syntax=docker/dockerfile:1

# --- stage 1: dependency recipe ---
FROM rust:slim-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

# --- stage 2: compute recipe ---
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- stage 3: build ---
FROM chef AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        make \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# build dependencies first (cached layer)
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# build the project
COPY . .
RUN cargo build --release

# --- stage 4: runtime ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home --shell /usr/sbin/nologin hydrant

COPY --from=builder /build/target/release/hydrant /usr/local/bin/hydrant

# data volume for the fjall database
VOLUME ["/data"]
WORKDIR /data

ENV HYDRANT_DATABASE_PATH=/data/hydrant.db

EXPOSE 3000

USER hydrant

ENTRYPOINT ["/usr/local/bin/hydrant"]
