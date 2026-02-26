# go-ds-flatfs

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![Coverage Status](https://img.shields.io/codecov/c/github/ipfs/go-ds-flatfs.svg)](https://codecov.io/gh/ipfs/go-ds-flatfs)
[![Build Status](https://img.shields.io/github/actions/workflow/status/ipfs/go-ds-flatfs/go-test.yml?branch=master)](https://github.com/ipfs/go-ds-flatfs/actions)
[![GoDoc](https://pkg.go.dev/badge/github.com/ipfs/go-ds-flatfs)](https://pkg.go.dev/github.com/ipfs/go-ds-flatfs)


> A datastore implementation using sharded directories and flat files to store data

`go-ds-flatfs` is used by `kubo` to store raw block contents on disk. It supports several sharding functions (prefix, suffix, next-to-last/*).

It is _not_ a general-purpose datastore and has several important restrictions.
See the restrictions section for details.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Install

`go-ds-flatfs` can be used like any Go module:


```
import "github.com/ipfs/go-ds-flatfs"
```

## Usage

Check the [GoDoc module documentation](https://pkg.go.dev/github.com/ipfs/go-ds-flatfs) for an overview of this module's
functionality.

### Restrictions

FlatFS is a special-purpose datastore designed exclusively for content-addressed
storage (CID:block pairs). It is **not** a general-purpose key-value store.

#### Content-Addressed Storage Only

This datastore assumes that keys are content identifiers (CIDs) where the key
is derived from a cryptographic hash of the value. This means:

- The same key always maps to the same value (values are deterministic)
- Multiple writes to the same key are idempotent (writing the same data twice is safe)

**Write semantics**: When multiple writes target the same key (concurrently or
within a batch), flatfs uses "first-successful-writer-wins" semantics. The first
write to complete is persisted; subsequent writes to the same key are silently
skipped. This is safe for content-addressed data because identical keys
guarantee identical values.

**If you need different semantics**: For data where the same key may have
different values over time (e.g., mutable metadata, counters, queues), use a
different datastore implementation such as
[go-ds-leveldb](https://github.com/ipfs/go-ds-leveldb) or
[go-ds-pebble](https://github.com/ipfs/go-ds-pebble), which provide
last-writer-wins semantics.

#### Key Format

FlatFS keys are severely restricted. Only keys that match `/[0-9A-Z+-_=]\+` are
allowed. That is, keys may only contain upper-case alpha-numeric characters,
'-', '+', '_', and '='. This is because values are written directly to the
filesystem without encoding.

Importantly, this means namespaced keys (e.g., /FOO/BAR), are _not_ allowed.
Attempts to write to such keys will result in an error.

### DiskUsage and Accuracy

This datastore implements the [`PersistentDatastore`](https://godoc.org/github.com/ipfs/go-datastore#PersistentDatastore) interface. It offers a `DiskUsage()` method which strives to find a balance between accuracy and performance. This implies:

* The total disk usage of a datastore is calculated when opening the datastore
* The current disk usage is cached frequently in a file in the datastore root (`diskUsage.cache` by default). This file is also
written when the datastore is closed.
* If this file is not present when the datastore is opened:
  * The disk usage will be calculated by walking the datastore's directory tree and estimating the size of each folder.
  * This may be a very slow operation for huge datastores or datastores with slow disks
  * The operation is time-limited (5 minutes by default).
  * Upon timeout, the remaining folders will be assumed to have the average of the previously processed ones.
* After opening, the disk usage is updated in every write/delete operation.

This means that for certain datastores (huge ones, those with very slow disks or special content), the values reported by
`DiskUsage()` might be reduced accuracy and the first startup (without a `diskUsage.cache` file present), might be slow.

If you need increased accuracy or a fast start from the first time, you can manually create or update the
`diskUsage.cache` file.

The file `diskUsage.cache` is a JSON file with two fields `diskUsage` and `accuracy`.  For example the JSON file for a
small repo might be:

```
{"diskUsage":6357,"accuracy":"initial-exact"}
```

`diskUsage` is the calculated disk usage and `accuracy` is a note on the accuracy of the initial calculation.  If the
initial calculation was accurate the file will contain the value `initial-exact`.  If some of the directories have too
many entries and the disk usage for that directory was estimated based on the first 2000 entries, the file will contain
`initial-approximate`.  If the calculation took too long and timed out as indicated above, the file will contain
`initial-timed-out`.

If the initial calculation timed out the JSON file might be:
```
{"diskUsage":7589482442898,"accuracy":"initial-timed-out"}

```

To fix this with a more accurate value you could do (in the datastore root):

    $ du -sb .
    7536515831332    .
    $ echo -n '{"diskUsage":7536515831332,"accuracy":"initial-exact"}' > diskUsage.cache

## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © Protocol Labs, Inc.
