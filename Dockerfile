# Build stage
FROM golang:alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./
# (If you add external dependencies later, you'd also copy go.sum and run `go mod download` here)

# Copy source code
COPY . .

# Build the binary
# CGO_ENABLED=0 ensures a static binary
# -trimpath and -ldflags for smaller binary size
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o server ./cmd/server

# Final stage - using a tiny distroless or alpine base
FROM alpine:latest

WORKDIR /app

# Add tzdata for accurate timezone support if needed by logs
RUN apk --no-cache add tzdata

# Create output directory for processed images
RUN mkdir -p /app/output && chmod 777 /app/output

# Copy the static binary from builder
COPY --from=builder /app/server .

# Expose the default port
EXPOSE 8080

# Run the server
CMD ["./server", "-addr", ":8080", "-workers", "4", "-queue", "16", "-output", "/app/output"]
