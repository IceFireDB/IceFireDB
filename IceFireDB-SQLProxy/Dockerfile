# IceFireDB-SQLProxy lives in the repository's root Go module (it has no go.mod of its
# own), so the build context must be the repository root:
#
#   docker build -f IceFireDB-SQLProxy/Dockerfile -t icefiredb/sqlproxy:1.0.0 .
#
# Building from this directory alone will fail, because the module files are one level up.

FROM golang:1.27.3 AS builder

WORKDIR /src

# Dependencies first so a source-only change reuses this layer.
COPY go.mod go.sum ./
COPY vendor/ vendor/
COPY IceFireDB-SQLProxy/ IceFireDB-SQLProxy/

ARG VERSION=1.0.0
ARG COMMIT_HASH=unknown
ARG BUILD_DATE=unknown

# CGO is off so the binary runs on a minimal base image; the proxy speaks only TCP.
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath \
        -ldflags "-s -w -X main.BuildVersion=${COMMIT_HASH} -X main.BuildDate=${BUILD_DATE}" \
        -o /out/IceFireDB-SQLProxy ./IceFireDB-SQLProxy

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    adduser -D -H -u 10001 sqlproxy

WORKDIR /app
COPY --from=builder /out/IceFireDB-SQLProxy /app/IceFireDB-SQLProxy
COPY --from=builder /src/IceFireDB-SQLProxy/config/config.yaml /app/config/config.yaml

USER sqlproxy
EXPOSE 33306

ENTRYPOINT ["/app/IceFireDB-SQLProxy"]
CMD ["-c", "/app/config/config.yaml"]
