PROG=bin/Icefiredb-PubSub


SRCS=./cmd/proxy

# git commit hash
COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")

# Compile the date
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')
# compile
CFLAGS = -ldflags "-s -w -X \"main.BuildVersion=${COMMIT_HASH}\""
# GOOS 方便在Mac系统上快速的进行调试开发
GOOS = $(shell uname | tr 'A-Z' 'a-z')
# GOARCH 考虑到目前大部分机器为 64 的指令集，暂不对此进行处理

all:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	GOPROXY=$(GOPROXY) GOOS=$(GOOS) GOARCH=amd64 go build $(CFLAGS) -o $(PROG) $(SRCS)

# Compiling the RACE version
race:
	if [ ! -d "./bin/" ]; then \
    	mkdir bin; \
    	fi
	GOPROXY=$(GOPROXY) go build $(CFLAGS) -race -o $(PROG) $(SRCS)

clean:
	rm -rf ./bin

run:
	GOPROXY=$(GOPROXY) go run --race cmd/proxy/main.go -c config/config.yaml