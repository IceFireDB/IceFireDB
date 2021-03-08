module github.com/gitsrc/IceFireDB

go 1.16

require (
	github.com/ledisdb/ledisdb v0.0.0-00010101000000-000000000000
	github.com/onsi/ginkgo v1.15.0 // indirect
	github.com/onsi/gomega v1.10.5 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/syndtr/goleveldb v1.0.0
	github.com/tidwall/redcon v1.4.0
	github.com/tidwall/sds v0.1.0
	github.com/tidwall/uhaha v0.6.2
)

replace github.com/ledisdb/ledisdb v0.0.0-00010101000000-000000000000 => github.com/gitsrc/ledisdb v0.0.0-20210308111412-4981104d208c
