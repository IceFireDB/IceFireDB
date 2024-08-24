// Package multicodec exposes the multicodec table as Go constants.
package multicodec

//go:generate go run gen.go
//go:generate gofmt -w code.go
//go:generate stringer -type=Code -linecomment
