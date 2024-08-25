FUZZTIME ?= 1m

.PHONY: test

default: all

all: build test

clean:
	go clean ./...

build:
	go build ./...

test:
	go test ./...

fuzz:
	go test ./... -fuzz=Fuzz -fuzztime $(FUZZTIME)