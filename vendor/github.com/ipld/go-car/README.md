go-car (go!)
==================

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![](https://img.shields.io/badge/project-ipld-orange.svg?style=flat-square)](https://github.com/ipld/ipld)
[![](https://img.shields.io/badge/matrix-%23ipld-blue.svg?style=flat-square)](https://matrix.to/#/#ipld:ipfs.io)
[![Go Reference](https://pkg.go.dev/badge/github.com/ipld/go-car.svg)](https://pkg.go.dev/github.com/ipld/go-car)
[![Coverage Status](https://codecov.io/gh/ipld/go-car/branch/master/graph/badge.svg)](https://codecov.io/gh/ipld/go-car/branch/master)

> Work with car (Content addressed ARchive) files!

This is a Golang implementation of the [CAR specifications](https://ipld.io/specs/transport/car/), both [CARv1](https://ipld.io/specs/transport/car/carv1/) and [CARv2](https://ipld.io/specs/transport/car/carv2/).

As a format, there are two major module versions:

* [`go-car/v2`](v2/) is geared towards reading and writing CARv2 files, and also
  supports consuming CARv1 files and using CAR files as an IPFS blockstore.
* `go-car`, in the root directory, only supports reading and writing CARv1 files.

Most users should use v2, especially for new software, since the v2 API transparently supports both CAR formats.

## Usage / Installation

This repository provides a `car` binary that can be used for creating, extracting, and working with car files.

To install the latest version of `car`, run:
```shell script
go install github.com/ipld/go-car/cmd/car@latest
```

More information about this binary is available in [`cmd/car`](cmd/car/)


## Features

[CARv2](v2) features:
* [Generate index](https://pkg.go.dev/github.com/ipld/go-car/v2#GenerateIndex) from an existing CARv1 file
* [Wrap](https://pkg.go.dev/github.com/ipld/go-car/v2#WrapV1) CARv1 files into a CARv2 with automatic index generation.
* Random-access to blocks in a CAR file given their CID via [Read-Only blockstore](https://pkg.go.dev/github.com/ipld/go-car/v2/blockstore#NewReadOnly) API, with transparent support for both CARv1 and CARv2
* Write CARv2 files via [Read-Write blockstore](https://pkg.go.dev/github.com/ipld/go-car/v2/blockstore#OpenReadWrite) API, with support for appending blocks to an existing CARv2 file, and resumption from a partially written CARv2 files.
* Individual access to [inner CARv1 data payload]((https://pkg.go.dev/github.com/ipld/go-car/v2#Reader.DataReader)) and [index]((https://pkg.go.dev/github.com/ipld/go-car/v2#Reader.IndexReader)) of a CARv2 file via the `Reader` API.


## API Documentation

See docs on [pkg.go.dev](https://pkg.go.dev/github.com/ipld/go-car).

## Examples

Here is a shortlist of other examples from the documentation

* [Wrap an existing CARv1 file into an indexed CARv2 file](https://pkg.go.dev/github.com/ipld/go-car/v2#example-WrapV1File)
* [Open read-only blockstore from a CAR file](https://pkg.go.dev/github.com/ipld/go-car/v2/blockstore#example-OpenReadOnly)
* [Open read-write blockstore from a CAR file](https://pkg.go.dev/github.com/ipld/go-car/v2/blockstore#example-OpenReadWrite)
* [Read the index from an existing CARv2 file](https://pkg.go.dev/github.com/ipld/go-car/v2/index#example-ReadFrom)
* [Extract the index from a CARv2 file and store it as a separate file](https://pkg.go.dev/github.com/ipld/go-car/v2/index#example-WriteTo)

## Maintainers

* [Masih Derkani](https://github.com/masih)
* [Will Scott](https://github.com/willscott)

## Contribute

PRs are welcome!

When editing the Readme, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

Apache-2.0/MIT © Protocol Labs
