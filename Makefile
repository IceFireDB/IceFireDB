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
