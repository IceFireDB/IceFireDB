package asnutil

import (
	_ "embed"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"strconv"
)

//go:embed sorted-network-list.bin
var dataset string

const entrySize = 8*2 + 4 // start, end 8 bytes; asn 4 bytes

func readEntry(index uint) (start, end uint64, asn uint32) {
	base := entrySize * index
	b := dataset[base : base+entrySize]
	start = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
	b = b[8:]
	end = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
	b = b[8:]
	asn = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return
}

// AsnForIPv6 returns the AS number for the given IPv6 address.
// If no mapping exists for the given network, this function will return a zero ASN number.
func AsnForIPv6(ip net.IP) (asn uint32) {
	ip = ip.To16()
	if ip == nil {
		return
	}
	return AsnForIPv6Network(binary.BigEndian.Uint64(ip))
}

func init() {
	if len(dataset) > math.MaxUint/2 {
		panic("list is too big and would overflow in binary search")
	}
}

// AsnForIPv6Network returns the AS number for the given IPv6 network.
// If no mapping exists for the given network, this function will return a zero ASN number.
// network is the first 64 bits of the ip address interpreted as big endian.
func AsnForIPv6Network(network uint64) (asn uint32) {
	n := uint(len(dataset)) / entrySize
	var i, j uint = 0, n
	for i < j {
		h := (i + j) / 2 // wont overflow since the list can't be that large
		start, end, asn := readEntry(h)
		if start <= network {
			if network <= end {
				return asn
			}
			i = h + 1
		} else {
			j = h
		}
	}
	if i >= n {
		return 0
	}
	start, end, asn := readEntry(i)
	if start <= network && network <= end {
		return asn
	}
	return 0
}

// Deprecated: use [AsnForIPv6] or [AsnForIPv6Network], they do not allocate.
var Store backwardCompat

type backwardCompat struct{}

// AsnForIPv6 returns the AS number for the given IPv6 address.
// If no mapping exists for the given IP, this function will
// return an empty ASN and a nil error.
func (backwardCompat) AsnForIPv6(ip net.IP) (string, error) {
	ip = ip.To16()
	if ip == nil {
		return "", errors.New("ONLY IPv6 addresses supported")
	}

	asn := AsnForIPv6Network(binary.BigEndian.Uint64(ip))
	if asn == 0 {
		return "", nil
	}
	return strconv.FormatUint(uint64(asn), 10), nil
}

func (backwardCompat) Init() {}
