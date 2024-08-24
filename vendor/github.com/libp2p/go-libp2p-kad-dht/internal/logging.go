package internal

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
)

func multibaseB32Encode(k []byte) string {
	res, err := multibase.Encode(multibase.Base32, k)
	if err != nil {
		// Should be unreachable
		panic(err)
	}
	return res
}

func tryFormatLoggableRecordKey(k string) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("LoggableRecordKey is empty")
	}
	var proto, cstr string
	if k[0] == '/' {
		// it's a path (probably)
		protoEnd := strings.IndexByte(k[1:], '/')
		if protoEnd < 0 {
			return "", fmt.Errorf("LoggableRecordKey starts with '/' but is not a path: %s", multibaseB32Encode([]byte(k)))
		}
		proto = k[1 : protoEnd+1]
		cstr = k[protoEnd+2:]

		encStr := multibaseB32Encode([]byte(cstr))
		return fmt.Sprintf("/%s/%s", proto, encStr), nil
	}

	return "", fmt.Errorf("LoggableRecordKey is not a path: %s", multibaseB32Encode([]byte(cstr)))
}

type LoggableRecordKeyString string

func (lk LoggableRecordKeyString) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableRecordKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

type LoggableRecordKeyBytes []byte

func (lk LoggableRecordKeyBytes) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableRecordKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

type LoggableProviderRecordBytes []byte

func (lk LoggableProviderRecordBytes) String() string {
	newKey, err := tryFormatLoggableProviderKey(lk)
	if err == nil {
		return newKey
	}
	return err.Error()
}

func tryFormatLoggableProviderKey(k []byte) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("LoggableProviderKey is empty")
	}

	encodedKey := multibaseB32Encode(k)

	// The DHT used to provide CIDs, but now provides multihashes
	// TODO: Drop this when enough of the network has upgraded
	if _, err := cid.Cast(k); err == nil {
		return encodedKey, nil
	}

	if _, err := multihash.Cast(k); err == nil {
		return encodedKey, nil
	}

	return "", fmt.Errorf("LoggableProviderKey is not a Multihash or CID: %s", encodedKey)
}
