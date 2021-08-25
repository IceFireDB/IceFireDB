PROG=bin/IceFireDB


SRCS=.

# 安装目录
INSTALL_PREFIX=/usr/local/IceFireDB

# 配置安装的目录
CONF_INSTALL_PREFIX=/usr/local/IceFireDB

# git commit hash
COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")

# 编译日期
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')

# 编译条件
CFLAGS = -ldflags "-s -w -X \"main.BuildVersion=${COMMIT_HASH}\" -X \"main.BuildDate=$(BUILD_DATE)\""

all:
	if [ ! -d "./bin/" ]; then \
	mkdir bin; \
	fi
	go build $(CFLAGS) -o $(PROG) $(SRCS)

# 编译race版本
race:
	if [ ! -d "./bin/" ]; then \
    	mkdir bin; \
    	fi
	go build $(CFLAGS) -race -o $(PROG) $(SRCS)

# release 版本
RELEASE_DATE = $(shell date '+%Y%m%d%H%M%S')
RELEASE_VERSION = $(shell git rev-parse --short HEAD || echo "GitNotFound")
RELEASE_DIR=release_bin
RELEASE_BIN_NAME=IceFireDB
release:
	if [ ! -d "./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)" ]; then \
	mkdir -p ./$(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION); \
	fi
	go build  $(CFLAGS) -o $(RELEASE_DIR)/$(RELEASE_DATE)_$(RELEASE_VERSION)/$(RELEASE_BIN_NAME)_linux_amd64 $(SRCS)

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
	go test ./...