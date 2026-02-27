# Stage 1: Build the Rust binary and sqlx-cli
FROM rust:1-bookworm AS builder

WORKDIR /build

RUN cargo install sqlx-cli --no-default-features --features postgres

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN cargo build --release && rm -rf src target/release/deps/harvester* target/release/deps/libharvester*

COPY src/ src/
COPY migrations/ migrations/
COPY .sqlx/ .sqlx/
ENV SQLX_OFFLINE=true
RUN cargo build --release

# Stage 2: Ruby gems (traject + arclight helpers)
FROM ruby:4-slim-bookworm AS gems

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libxml2-dev \
    libxslt-dev \
    libyaml-dev \
    && rm -rf /var/lib/apt/lists/*

RUN gem install arclight --no-document

# Stage 3: Runtime image
FROM ruby:4-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libxml2 \
    libxslt1.1 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/sqlx /usr/local/bin/sqlx
COPY --from=builder /build/target/release/harvester /usr/local/bin/harvester
COPY --from=gems /usr/local/bundle /usr/local/bundle

COPY fixtures/rules.txt /app/rules/default.txt
COPY migrations/ /app/migrations/
COPY traject/ /app/traject/

COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

RUN mkdir -p /app/data && chown -R 1000:1000 /app
USER 1000:1000
WORKDIR /app

ENTRYPOINT ["docker-entrypoint.sh"]
