# `sds`

[![GoDoc](https://godoc.org/github.com/tidwall/sds?status.svg)](https://godoc.org/github.com/tidwall/sds)

This package provides a fast and simple way for reading and writing custom
data streams in Go.

Supports reading and writing most basic types including: 
`int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`,
`byte`, `bool`, `float32`, `float64`, `[]byte`, `string`.
Also `uvarint` and `varint`. 

## Usage

### Installing

To start using `sds`, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/sds
```

### Basic operations

```go
// create a writer
var bb bytes.Buffer
w := sds.NewWriter(&bb) 

// write some stuff
err = w.WriteString("Hello Jello")
err = w.WriteBytes(someBinary)
err = w.WriteUvarint(8589869056)
err = w.WriteVarint(-119290019)
err = w.WriteUint16(-119290019)

// close the reader when done
w.Flush()

// create a reader
r := sds.NewReader(&bb)

// read some stuff
s, err = w.ReadString()
b, err = w.ReadBytes()
x, err = w.ReadUvarint()
x, err = w.ReadVarint()
x, err = w.ReadUint16()
```

*Now isn't that nice.*

## License

`sds` source code is available under the MIT License.
