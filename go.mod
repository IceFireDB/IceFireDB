module github.com/gitsrc/IceFireDB

go 1.16

require (
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/dgraph-io/ristretto v0.1.0
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/go-redis/redis/v8 v8.11.3
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/go-hclog v0.16.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6
	github.com/pelletier/go-toml v1.8.1 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/spf13/cast v1.4.1
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/syndtr/goleveldb v1.0.0
	github.com/tidwall/raft-leveldb v0.2.0 // indirect
	github.com/tidwall/redcon v1.4.1
	github.com/tidwall/sds v0.1.0
	github.com/tidwall/uhaha v0.8.1
	golang.org/x/crypto v0.0.0-20210220033148-5ea612d1eb83 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
)

replace (
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6 => github.com/gitsrc/ledisdb v0.0.0-20210311085546-2e33308de99f
	github.com/tidwall/uhaha v0.8.1 => github.com/gitsrc/uhaha v0.6.2-0.20210827055200-e2d63f4d4aee
)
