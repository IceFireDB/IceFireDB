module github.com/gitsrc/IceFireDB

go 1.16

require (
	github.com/IceFireDB/kit v0.0.0-20210927091721-f0b91a5b05fb
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/dgraph-io/ristretto v0.1.0
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/go-redis/redis/v8 v8.11.3
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/go-hclog v0.16.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/juju/loggo v0.0.0-20190526231331-6e530bcce5d8 // indirect
	github.com/juju/testing v0.0.0-20191001232224-ce9dec17d28b // indirect
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6
	github.com/pelletier/go-toml v1.8.1 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/goredis v0.0.0-20180423163523-0b4019cbd7b7
	github.com/spf13/cast v1.4.1
	github.com/syndtr/goleveldb v1.0.0
	github.com/tidwall/raft-leveldb v0.2.0 // indirect
	github.com/tidwall/redcon v1.4.1
	github.com/tidwall/uhaha v0.8.1
	golang.org/x/crypto v0.0.0-20210220033148-5ea612d1eb83 // indirect
	golang.org/x/sys v0.0.0-20210603081109-ebe580a85c40 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
)

replace (
	github.com/ledisdb/ledisdb => github.com/gitsrc/ledisdb v0.0.0-20210924091748-05ada23e89c0
	github.com/tidwall/uhaha v0.8.1 => github.com/gitsrc/uhaha v0.6.2-0.20210907032229-eef8220ec1f7
)
