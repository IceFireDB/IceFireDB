PROG=bin/IceFireDB

DRIVER?=badger

SRCS=.

# install dir
INSTALL_PREFIX=/usr/local/IceFireDB

# install config dir
CONF_INSTALL_PREFIX=/usr/local/IceFireDB

# git commit hash
COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")

# build date
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')

# build cflags
CFLAGS = -ldflags "-s -w -X \"main.BuildVersion=${COMMIT_HASH}\" -X \"main.BuildDate=$(BUILD_DATE)\""

all: build-linux-amd64 build-linux-arm64 build-linux-armv5 build-linux-armv6 build-linux-armv7

# build race version
race:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	go build $(CFLAGS) -race -o $(PROG) $(SRCS)

# release version
RELEASE_DATE = $(shell date '+%Y%m%d%H%M%S')
RELEASE_VERSION = $(shell git rev-parse --short HEAD || echo "GitNotFound")
RELEASE_DIR=release_bin
RELEASE_BIN_NAME=IceFireDB
release: build-release-linux-amd64 build-release-linux-arm64 build-release-linux-armv5 build-release-linux-armv6 build-release-linux-armv7
	if [ ! -d "./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)" ]; then \
	mkdir -p ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION); \
	fi
	cp ./bin/$(RELEASE_BIN_NAME)_linux_amd64 ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)/
	cp ./bin/$(RELEASE_BIN_NAME)_linux_arm64 ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)/
	cp ./bin/$(RELEASE_BIN_NAME)_linux_armv5 ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)/
	cp ./bin/$(RELEASE_BIN_NAME)_linux_armv6 ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)/
	cp ./bin/$(RELEASE_BIN_NAME)_linux_armv7 ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)/

install:
	cp $(PROG) $(INSTALL_PREFIX)/bin

	if [ ! -d "${CONF_INSTALL_PREFIX}" ]; then \
	mkdir $(CONF_INSTALL_PREFIX); \
	fi

	cp -R config/* $(CONF_INSTALL_PREFIX)

clean:
	rm -rf ./bin

	rm -rf $(INSTALL_PREFIX)/bin/IceFireDB

	rm -rf $(CONF_INSTALL_PREFIX)

run:
	go run .

run_dev:
	go run .

test:
	DRIVER=$(DRIVER) go test -v --v ./...

test-compat:
	DRIVER=$(DRIVER) go test -v -count=1 -tags alltest ./

# Integration tests launch the real binary as subprocesses to exercise crash
# recovery (SIGKILL) and multi-node Raft failover. Builds the binary first.
test-integration:
	go build -o /tmp/icefiredb-it .
	IFDB_BIN=/tmp/icefiredb-it go test -v -count=1 -tags integration -run TestIntegration ./

# Partition test runs a 3-node cluster in Docker containers and cuts the leader
# off the inter-node network to verify Raft's no-split-brain guarantee. Requires
# docker; builds a static linux binary for the containers.
test-partition:
	CGO_ENABLED=0 go build -o /tmp/icefiredb-static .
	IFDB_STATIC=/tmp/icefiredb-static go test -v -count=1 -tags partition -run TestPartition ./

# Sustained soak/load test against a 3-node cluster. Tune via environment:
#   SOAK_DURATION (e.g. 10m), SOAK_WORKERS (e.g. 8), SOAK_CHAOS=1 (cycle leaders).
# Example: SOAK_DURATION=10m SOAK_CHAOS=1 make soak
soak:
	go build -o /tmp/icefiredb-it .
	IFDB_BIN=/tmp/icefiredb-it go test -v -count=1 -timeout 0 -tags integration -run TestIntegrationSoak ./

bench-run:
	rm -rf ./data
	./bin/IceFireDB --nosync

# Build for Linux AMD64
build-linux-amd64 build-release-linux-amd64:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	GOOS=linux GOARCH=amd64 go build $(CFLAGS) -o ./bin/$(RELEASE_BIN_NAME)_linux_amd64 $(SRCS)

# Build for Linux ARM64
build-linux-arm64 build-release-linux-arm64:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	GOOS=linux GOARCH=arm64 go build $(CFLAGS) -o ./bin/$(RELEASE_BIN_NAME)_linux_arm64 $(SRCS)

# Build for Linux ARMv5
build-linux-armv5 build-release-linux-armv5:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	GOOS=linux GOARCH=arm GOARM=5 go build $(CFLAGS) -o ./bin/$(RELEASE_BIN_NAME)_linux_armv5 $(SRCS)

# Build for Linux ARMv6
build-linux-armv6 build-release-linux-armv6:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	GOOS=linux GOARCH=arm GOARM=6 go build $(CFLAGS) -o ./bin/$(RELEASE_BIN_NAME)_linux_armv6 $(SRCS)

# Build for Linux ARMv7
build-linux-armv7 build-release-linux-armv7:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	GOOS=linux GOARCH=arm GOARM=7 go build $(CFLAGS) -o ./bin/$(RELEASE_BIN_NAME)_linux_armv7 $(SRCS)

# Build for local environment
localbuild:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	go build $(CFLAGS) -o ./bin/$(RELEASE_BIN_NAME) $(SRCS)