# ==========================================
# Multi-Stage Dockerfile for AI Go Benchmark
# ==========================================

# Stage 1: Build binary bot & cli
FROM golang:bookworm AS builder
ENV GOTOOLCHAIN=auto

WORKDIR /build

# Cache dependency layer
COPY go.mod go.sum ./
RUN go mod download

# Copy full source code
COPY . .

# Build application binaries with CGO enabled (for SQLite/Turso and Go race detector)
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /bin/benchmark-bot ./cmd/bot
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /bin/benchmark-cli ./cmd/cli

# Stage 2: Production Runtime
# NOTE: Menggunakan base golang agar runtime sandbox memiliki 'go build -race'
# dan gcc C compiler untuk menguji kode Go yang dihasilkan oleh AI secara dinamis.
FROM golang:bookworm-slim AS runner
ENV GOTOOLCHAIN=auto

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gcc \
    libc6-dev \
    curl \
    git \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Salin binary dari stage builder
COPY --from=builder /bin/benchmark-bot /usr/local/bin/benchmark-bot
COPY --from=builder /bin/benchmark-cli /usr/local/bin/benchmark-cli

# Port internal untuk HTTP Health Check & Observability
EXPOSE 8080

# Liveness & readiness probe
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

# Jalankan Telegram Bot sebagai entrypoint default
ENTRYPOINT ["/usr/local/bin/benchmark-bot"]
