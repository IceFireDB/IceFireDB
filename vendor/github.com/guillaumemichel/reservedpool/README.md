# reservedpool

[![Go Reference](https://pkg.go.dev/badge/github.com/guillaumemichel/reservedpool.svg)](https://pkg.go.dev/github.com/guillaumemichel/reservedpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/guillaumemichel/reservedpool)](https://goreportcard.com/report/github.com/guillaumemichel/reservedpool)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Build Status](https://github.com/guillaumemichel/reservedpool/actions/workflows/ci.yml/badge.svg)](https://github.com/guillaumemichel/reservedpool/actions/workflows/ci.yml)

`reservedpool` provides a small concurrency limiting pool that reserves a
number of slots per category. Each category can always consume up to its
reserved limit, and any remaining capacity is shared among all
categories.

## Install

```sh
go get github.com/guillaumemichel/reservedpool
```

## Example

```go
package main

import (
  "fmt"
  "sync"

  "github.com/guillaumemichel/reservedpool"
)

func main() {
  // Create a pool allowing up to 4 concurrent workers with
  // 2 slots reserved for category "a" and 1 for category "b".
  p := reservedpool.New(4, map[string]int{"a": 2, "b": 1})

  n := 6
  wg := sync.WaitGroup{}
  wg.Add(n)
  for i := range n {
    go func(i int) {
      defer wg.Done()
      if err := p.Acquire("a"); err != nil {
        return
      }
      defer p.Release("a")
      fmt.Println("running job", i)
      // Do some work ...
    }(i)
  }

  wg.Wait()
  p.Close()
}
```

See [godoc](https://pkg.go.dev/github.com/guillaumemichel/reservedpool) for
full documentation.
