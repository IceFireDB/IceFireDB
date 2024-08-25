.PHONY: all
all: test lint

.PHONY: lint
lint:
	golangci-lint run  -v ./...

.PHONY: test
test:
	go test -race -covermode=atomic -coverpkg=all -coverprofile=coverage.txt -cover -v ./... -test.timeout=5m
	cd example && go test -v ./...

.PHONY: goleak
goleak:
	go test -tags=goleak -v ./...
