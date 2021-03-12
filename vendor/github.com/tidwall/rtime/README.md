# rtime

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/tidwall/rtime)

Retrieve the current time from remote servers.

It works by requesting timestamps from twelve very popular hosts over https.
As soon as it gets at least three responses, it takes the two that have the
smallest difference in time. And from those two it picks the one that is
the oldest. Finally it ensures that the time is monotonic.

## Getting

```
go get -u github.com/tidwall/rtime
```

## Using

Get the remote time with `rtime.Now()`.

```go
tm := rtime.Now()
if tm.IsZero() {
    panic("internet offline")
}
println(tm.String())
// output: 2020-03-29 10:27:00 -0700 MST
}
```

## Stay in sync

The `rtime.Now()` will be a little slow, usually 200 ms or more, because it
must make a round trip to three or more remote servers to determine the correct
time. 

You make it fast like the built-in `time.Now()` by calling `rtime.Sync()` once
at the start of your application.

```go
if err := rtime.Sync(); err != nil {
    panic(err)
}
// All following rtime.Now() calls will now be quick and without the need for
// checking its result.
tm := rtime.Now()
println(tm.String())
```

It's a good idea to call `rtime.Sync()` at the top of the `main()` or `init()`
functions.

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).