module github.com/gitsrc/IceFireDB

go 1.16

require (
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/gitsrc/rafthub v0.0.0-20210819144721-a3eaa0203ad4
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6
	github.com/onsi/ginkgo v1.15.0 // indirect
	github.com/onsi/gomega v1.10.5 // indirect
	github.com/pelletier/go-toml v1.8.1 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/syndtr/goleveldb v1.0.0
	github.com/tidwall/redcon v1.4.1
	github.com/tidwall/sds v0.1.0
	github.com/tidwall/uhaha v0.8.1
	golang.org/x/crypto v0.0.0-20210220033148-5ea612d1eb83 // indirect
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110 // indirect
	golang.org/x/sys v0.0.0-20210309074719-68d13333faf2 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
)

replace github.com/ledisdb/ledisdb v0.0.0-20200510135210-d35789ec47e6 => github.com/gitsrc/ledisdb v0.0.0-20210311085546-2e33308de99f
