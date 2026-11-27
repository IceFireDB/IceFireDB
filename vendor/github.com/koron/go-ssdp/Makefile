TEST_PACKAGE ?= ./...

.PHONY: build
build:
	@if [ -n "$$(go list -f '{{if (eq .Name "main")}}{{.ImportPath}}{{end}}' ./...)" ] ; then \
	  ( set -x ; go build -o _build/ -gcflags '-e' ./... ) ; \
	else \
	  ( set -x ; go build -gcflags '-e' ./... ) ; \
	fi

.PHONY: test
test:
	go test $(TEST_PACKAGE)

.PHONY: race
race:
	go test -race $(TEST_PACKAGE)

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
clean:
	go clean ./...
	rm -rf _build/
	rm -f tags
	rm -f tmp/_cover.out tmp/cover.html

.PHONY: upgradable
upgradable:
	@go list -m -mod=readonly -u -f='{{if and (not .Indirect) (not .Main)}}{{if .Update}}{{.Path}}@{{.Update.Version}} [{{.Version}}]{{else if .Replace}}{{if .Replace.Update}}{{.Path}}@{{.Replace.Update.Version}} [replaced:{{.Replace.Version}} {{.Version}}]{{end}}{{end}}{{end}}' all

.PHONY: upgradable-all
upgradable-all:
	@go list -m -u -f '{{if .Update}}{{.Path}} {{.Version}} [{{.Update.Version}}]{{end}}' all

# based on: github.com/koron-go/_skeleton/Makefile
# $Hash:5740dbd08ad1f0dabc56331f39d2b32d2512dfcd36d26344add2c78e$
