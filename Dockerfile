# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git, build tools
RUN apk add --no-cache git ca-certificates

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/client ./cmd/client

# Runtime stage
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /app/server /app/server
COPY --from=builder /app/client /app/client

EXPOSE 50051 9090

ENTRYPOINT ["/app/server"]
