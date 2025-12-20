# Production Dockerfile for Go API
FROM golang:1.21-alpine AS builder

# Set Go environment
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
ENV GOPROXY=https://proxy.golang.org,direct
ENV GOSUMDB=sum.golang.org

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy source files
COPY main.go ./

# Initialize go module and install dependencies
# This avoids checksum issues by starting fresh
RUN go mod init sugestao-compra-api && \
    go get github.com/gin-gonic/gin@v1.9.1 && \
    go get github.com/lib/pq@v1.10.9 && \
    go mod tidy

# Build the application
RUN go build -a -installsuffix cgo -ldflags '-w -s' -o main main.go

# Production stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata wget

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/main ./main

# Change ownership
RUN chown appuser:appgroup main && chmod +x main

# Switch to non-root user
USER appuser

# Set timezone
ENV TZ=America/Sao_Paulo

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
CMD ["./main"]