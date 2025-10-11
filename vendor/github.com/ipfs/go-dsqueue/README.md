# go-dsqueue

[![Build Status](https://img.shields.io/github/actions/workflow/status/ipfs/go-dsqueue/go-test.yml?branch=main)](https://github.com/ipfs/go-dsqueue/actions)
[![GoDoc](https://pkg.go.dev/badge/github.com/ipfs/go-dsqueue)](https://pkg.go.dev/github.com/ipfs/go-dsqueue)


> Buffered FIFO interface to the datastore

The dsqueue package provides a buffered FIFO queue backed by a [Batching Datastore](https://pkg.go.dev/github.com/ipfs/go-datastore#Batching). Queued items are persisted in the datastore when the input buffer is full, after some amount of idle time, and when the queue is shuddown.

## Documentation

https://pkg.go.dev/github.com/ipfs/go-dsqueue

## Example

```go
ds = getBatchingDatastore() 
dsq := dsqueue.New(ds, "ExampleQueue")
defer dsq.Close()

c, err := cid.Decode("QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB")
if err != nil {
    panic(err)
}

dsq.Put(c.Bytes())

out := <-dsq.Out()
c2, err := cid.Parse(out)
if err != nil {
    panic(err)
}

if c2 != c {
    fmt.Fprintln(os.Stderr, "cids are not quual")
}
```

## Lead Maintainer

[@gammazero](https://github.com/gammazero)

## Contributing

Contributions are welcome! This repository is part of the IPFS project and therefore governed by our [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
