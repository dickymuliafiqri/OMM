# Multi-stage build
FROM golang:1.24-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /app/omm-bench ./cmd/bench

# Runtime: Debian slim dengan GCC untuk race detector
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Go toolchain (untuk go test -race di sandbox)
COPY --from=golang:1.24-bookworm /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /app
COPY --from=builder /app/omm-bench .

# Non-root user
RUN useradd -m -s /bin/bash ommuser
USER ommuser

EXPOSE 9090
CMD ["./omm-bench"]
