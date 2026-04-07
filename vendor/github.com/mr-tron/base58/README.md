# Fast Implementation of Base58 encoding
[![GoDoc](https://godoc.org/github.com/mr-tron/base58?status.svg)](https://godoc.org/github.com/mr-tron/base58)  [![Go Report Card](https://goreportcard.com/badge/github.com/mr-tron/base58)](https://goreportcard.com/report/github.com/mr-tron/base58)
[![Used By](https://sourcegraph.com/github.com/mr-tron/base58/-/badge.svg)](https://sourcegraph.com/github.com/mr-tron/base58?badge)

Fast implementation of base58 encoding in Go. 

Base algorithm is adapted from https://github.com/trezor/trezor-crypto/blob/master/base58.c

## Benchmark
- Trivial - encoding based on big.Int (most libraries use such an implementation)
- Fast - optimized algorithm provided by this module

```
cpu: Apple M4
BenchmarkTrivialBase58Encoding-10       	  712528	      1539 ns/op
BenchmarkFastBase58Encoding-10          	 6090552	       193.7 ns/op

BenchmarkTrivialBase58Decoding-10       	 1539550	       767.5 ns/op
BenchmarkFastBase58Decoding-10          	18001034	        64.68 ns/op
```
Encoding - **faster by 8 times**

Decoding - **faster by 12 times**

## Usage example

```go

package main

import (
	"fmt"
	"github.com/mr-tron/base58"
)

func main() {

	encoded := "1QCaxc8hutpdZ62iKZsn1TCG3nh7uPZojq"
	num, err := base58.Decode(encoded)
	if err != nil {
		fmt.Printf("Demo %v, got error %s\n", encoded, err)	
	}
	chk := base58.Encode(num)
	if encoded == string(chk) {
		fmt.Printf ( "Successfully decoded then re-encoded %s\n", encoded )
	} 
}

```
