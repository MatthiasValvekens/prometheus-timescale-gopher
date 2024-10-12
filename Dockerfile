# Build stage
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder
COPY ./pkg prometheus-adapter/pkg
COPY ./cmd prometheus-adapter/cmd
COPY ./go.mod prometheus-adapter/go.mod
COPY ./go.sum prometheus-adapter/go.sum
ARG TARGETOS
ARG TARGETARCH
RUN apk update && apk add --no-cache git \
    && cd prometheus-adapter \
    && GOOS=$TARGETOS GOARCH=$TARGETARCH go mod download \
    && GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -a --ldflags '-w' -o /go/prometheus-postgresql-adapter ./cmd/prometheus-postgresql-adapter

# Final image
FROM busybox
COPY --from=builder /go/prometheus-postgresql-adapter /
USER nobody

ENTRYPOINT ["/prometheus-postgresql-adapter"]
