EXAMPLES = advertise alive bye monitor search

.PHONY: build
build:
	go build -gcflags '-e'

.PHONY: test
test:
	go test $(TEST_PACKAGE)

.PHONY: bench
bench:
	go test -bench $(TEST_PACKAGE)

.PHONY: tags
tags:
	gotags -f tags -R .

.PHONY: cover
cover:
	mkdir -p tmp
	go test -coverprofile tmp/_cover.out $(TEST_PACKAGE)
	go tool cover -html tmp/_cover.out -o tmp/cover.html

.PHONY: checkall
checkall: vet staticcheck

.PHONY: vet
vet:
	go vet $(TEST_PACKAGE)

.PHONY: staticcheck
staticcheck:
	staticcheck $(TEST_PACKAGE)

.PHONY: clean
clean: examples-clean
	go clean
	rm -f tags
	rm -f tmp/_cover.out tmp/cover.html

# based on: github.com/koron-go/_skeleton/Makefile

.PHONY: test-race
test-race:
	go test -race .

.PHONY: examples
examples: examples-build

.PHONY: examples-build
examples-build: $(EXAMPLES)

.PHONY: examples-clean
examples-clean:
	rm -f $(EXAMPLES)

advertise: examples/advertise/*.go *.go
	go build ./examples/advertise

alive: examples/alive/*.go *.go
	go build ./examples/alive

bye: examples/bye/*.go *.go
	go build ./examples/bye

monitor: examples/monitor/*.go *.go
	go build ./examples/monitor

search: examples/search/*.go *.go
	go build ./examples/search
