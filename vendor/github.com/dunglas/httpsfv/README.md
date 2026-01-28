# httpsfv: Structured Field Values for HTTP in Go

This [Go (golang)](https://golang.org) library implements parsing and serialization for [Structured Field Values for HTTP (RFC 9651 and 8941)](https://httpwg.org/specs/rfc9651.html).

[![PkgGoDev](https://pkg.go.dev/badge/github.com/dunglas/httpsfv)](https://pkg.go.dev/github.com/dunglas/httpsfv)
![CI](https://github.com/dunglas/httpsfv/workflows/CI/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/dunglas/httpsfv/badge.svg?branch=master)](https://coveralls.io/github/dunglas/httpsfv?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/dunglas/httpsfv)](https://goreportcard.com/report/github.com/dunglas/httpsfv)

## Features

* Fully implementing the RFC
* Compliant with [the official test suite](https://github.com/httpwg/structured-field-tests)
* Unit and fuzz tested
* Strongly-typed
* Fast (see [the benchmark](httpwg_test.go))
* No dependencies

## Docs

[Browse the documentation on Go.dev](https://pkg.go.dev/github.com/dunglas/httpsfv).

## Credits

Created by [Kévin Dunglas](https://dunglas.fr). Sponsored by [Les-Tilleuls.coop](https://les-tilleuls.coop).
