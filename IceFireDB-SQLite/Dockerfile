FROM golang:1.17.8 AS builder

ENV GO111MODULE=on \
    GOPROXY=https://goproxy.cn,direct

WORKDIR /build

COPY . .

RUN CGO_ENABLED=1 go build -o sqlite .


FROM alpine:latest AS final

WORKDIR /app
COPY --from=builder /build/sqlite /app/
COPY --from=builder /build/config/config.yaml /app/config/
COPY --from=builder /build/db/read.txt /app/db/read.txt

ENTRYPOINT ["/app/sqlite"]
