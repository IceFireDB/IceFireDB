PROG=bin/IceFireDB-PubSub


SRCS=./cmd/pubsub

# git commit hash
COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")

# Compile the date
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')
# compile
CFLAGS = -ldflags "-s -w -X \"main.BuildVersion=${COMMIT_HASH}\""
# Get GOOS from System
LOCAL_OS := $(shell uname)
ifeq ($(LOCAL_OS),Linux)
   TARGET_OS_LOCAL = linux
else ifeq ($(LOCAL_OS),Darwin)
   TARGET_OS_LOCAL = darwin
else
   TARGET_OS_LOCAL = windows
   PROTOC_GEN_GO_NAME := "icefiredb.exe"
endif
export GOOS ?= $(TARGET_OS_LOCAL)

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
	GOPROXY=$(GOPROXY) go run --race cmd/pubsub/main.go -c config/config.yaml

run_dev:
	GOPROXY=$(GOPROXY) go run cmd/pubsub/main.go -c config/config_dev.yaml
