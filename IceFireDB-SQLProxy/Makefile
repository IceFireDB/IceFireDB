PROG=bin/IceFireDB-SQLProxy

# The proxy lives in the repository's root Go module, so every target runs from this
# directory but builds the module's package path.
PKG=./

# Version comes from internal/mysql/version.go so that the binary and the source cannot
# disagree about what 1.0.0 means.
VERSION=$(shell sed -n 's/^const Version = "\(.*\)"/\1/p' internal/mysql/version.go)
COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')

CFLAGS = -ldflags "-s -w -X \"main.BuildVersion=$(COMMIT_HASH)\" -X \"main.BuildDate=$(BUILD_DATE)\""

# Tags used by the Docker-based end-to-end suite.
E2E_TAGS=sqlproxe2e

.PHONY: build race vet test e2e docker clean run version

build:
	@mkdir -p bin
	go build $(CFLAGS) -o $(PROG) $(PKG)

race:
	@mkdir -p bin
	go build $(CFLAGS) -race -o $(PROG) $(PKG)

vet:
	go vet ./...

test:
	go test ./...

e2e:
	go test -tags $(E2E_TAGS) -count=1 -timeout 20m ./test/e2e/...

# The build context is the repository root, because this directory has no go.mod of its
# own: `docker build -f IceFireDB-SQLProxy/Dockerfile .`
docker:
	docker build -f Dockerfile -t icefiredb/sqlproxy:$(VERSION) .

clean:
	rm -rf ./bin

run:
	go run $(PKG) -c config/config.yaml

version:
	@echo $(VERSION)
