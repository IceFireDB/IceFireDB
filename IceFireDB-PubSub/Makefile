# Project name and target file
PROG = bin/IceFireDB-PubSub
SRCS = ./cmd/pubsub

# Get Git commit hash and build date
COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo "GitNotFound")
BUILD_DATE := $(shell date '+%Y-%m-%d %H:%M:%S')

# Compile flags
CFLAGS = -ldflags "-s -w -X \"main.BuildVersion=$(COMMIT_HASH)\" -X \"main.BuildDate=$(BUILD_DATE)\""

# Detect local OS type
LOCAL_OS := $(shell uname)
ifeq ($(LOCAL_OS),Linux)
    TARGET_OS = linux
else ifeq ($(LOCAL_OS),Darwin)
    TARGET_OS = darwin
else
    TARGET_OS = windows
    PROG := $(PROG).exe
endif

# Set GOOS environment variable
export GOOS ?= $(TARGET_OS)

# Default target: build the executable
all: $(PROG)

$(PROG): $(SRCS) FORCE
	@mkdir -p bin
	GOPROXY=$(GOPROXY) GOOS=$(GOOS) GOARCH=amd64 go build $(CFLAGS) -o $@ $(SRCS)

# Build with race detection
race: $(PROG)
	GOPROXY=$(GOPROXY) go build $(CFLAGS) -race -o $(PROG) $(SRCS)

# Clean generated files
clean:
	rm -rf ./bin

# Run with race detection
run:
	GOPROXY=$(GOPROXY) go run --race cmd/pubsub/main.go -c config/config.yaml

# Run with development configuration
run_dev:
	GOPROXY=$(GOPROXY) go run cmd/pubsub/main.go -c config/config_dev.yaml

# Force target
FORCE:

.PHONY: all race clean run run_dev FORCE
