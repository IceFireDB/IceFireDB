module github.com/gitsrc/IceFireDB

go 1.16

require (
	github.com/CodisLabs/codis v0.0.0-20181104082235-de1ad026e329
	github.com/IceFireDB/kit v0.0.0-20210915103202-283d718f5b02 // indirect
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/c4pt0r/cfg v0.0.0-20150302064018-429e6985f0b0
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/go-etcd v2.0.0+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/ristretto v0.1.0
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/garyburd/redigo v1.6.2
	github.com/go-redis/redis/v8 v8.11.3
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/go-hclog v0.16.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/juju/errors v0.0.0-20210818161939-5560c4c073ff
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6
	github.com/ledisdb/xcodis v0.0.0-20200426120518-40bcf4cf8a2d
	github.com/ngaut/go-zookeeper v0.0.0-20150813084940-9c3719e318c7
	github.com/ngaut/log v0.0.0-20210830112240-0124ec040aeb // indirect
	github.com/ngaut/logging v0.0.0-20150203141111-f98f5f4cd523
	github.com/ngaut/zkhelper v0.0.0-20151222125912-6738bdc138d4
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d
	github.com/pelletier/go-toml v1.8.1 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712 // indirect
	github.com/pkg/errors v0.9.1
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/spf13/cast v1.4.1
	github.com/syndtr/goleveldb v1.0.0
	github.com/tidwall/raft-leveldb v0.2.0 // indirect
	github.com/tidwall/redcon v1.4.1
	github.com/tidwall/sds v0.1.0
	github.com/tidwall/uhaha v0.8.1
	go.etcd.io/etcd/client/v2 v2.305.0
	golang.org/x/crypto v0.0.0-20210220033148-5ea612d1eb83 // indirect
	golang.org/x/net v0.0.0-20210913180222-943fd674d43e
	golang.org/x/sys v0.0.0-20210603081109-ebe580a85c40 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
)

replace (
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6 => github.com/gitsrc/ledisdb v0.0.0-20210311085546-2e33308de99f
	github.com/tidwall/uhaha v0.8.1 => github.com/gitsrc/uhaha v0.6.2-0.20210907032229-eef8220ec1f7
)
