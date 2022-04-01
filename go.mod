module github.com/gitsrc/IceFireDB

go 1.16

require (
	berty.tech/go-ipfs-log v1.5.0
	berty.tech/go-orbit-db v1.13.2
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/aws/aws-sdk-go v1.43.30
	github.com/cenkalti/backoff/v4 v4.1.2
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/dgraph-io/ristretto v0.1.0
	github.com/fatih/color v1.10.0 // indirect
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/go-hclog v0.16.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/ipfs/go-ipfs v0.12.1
	github.com/ipfs/go-ipfs-api v0.3.0
	github.com/ipfs/interface-go-ipfs-core v0.5.2
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6
	github.com/pelletier/go-toml v1.8.1 // indirect
	github.com/philippgille/gokv/encoding v0.6.0
	github.com/philippgille/gokv/test v0.6.0
	github.com/philippgille/gokv/util v0.6.0
	github.com/pkg/errors v0.9.1
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/spf13/cast v1.4.1
	github.com/syndtr/goleveldb v1.0.0
	github.com/tidwall/raft-leveldb v0.2.0 // indirect
	github.com/tidwall/redcon v1.4.4
	github.com/tidwall/sds v0.1.0
	github.com/tidwall/uhaha v0.8.1
)

replace (
	berty.tech/go-ipfs-log v1.5.0 => github.com/Jchicode/go-ipfs-log v0.0.0-20211229072901-39985a956db3
	berty.tech/go-orbit-db v1.13.2 => github.com/Jchicode/go-orbit-db v0.0.0-20211229073052-e4486e53a1da
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6 => github.com/gitsrc/ledisdb v0.0.0-20210311085546-2e33308de99f
	github.com/tidwall/uhaha v0.8.1 => github.com/gitsrc/uhaha v0.6.2-0.20210827055200-e2d63f4d4aee
)
