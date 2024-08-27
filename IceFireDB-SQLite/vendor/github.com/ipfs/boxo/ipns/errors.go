package ipns

import (
	"errors"
)

// MaxRecordSize is the IPNS Record [size limit].
//
// [size limit]: https://specs.ipfs.tech/ipns/ipns-record/#record-size-limit
const MaxRecordSize int = 10 << (10 * 1)

// ErrExpiredRecord is returned when an IPNS [Record] is invalid due to being expired.
var ErrExpiredRecord = errors.New("record is expired")

// ErrUnrecognizedValidity is returned when an IPNS [Record] has an unknown validity type.
var ErrUnrecognizedValidity = errors.New("record contains an unrecognized validity type")

// ErrInvalidValidity is returned when an IPNS [Record] has a known validity type,
// but the validity value is invalid.
var ErrInvalidValidity = errors.New("record contains an invalid validity")

// ErrRecordSize is returned when an IPNS [Record] exceeds the maximum size.
var ErrRecordSize = errors.New("record exceeds allowed size limit")

// ErrDataMissing is returned when an IPNS [Record] is missing the data field.
var ErrDataMissing = errors.New("record is missing the dag-cbor data field")

// ErrInvalidRecord is returned when an IPNS [Record] is malformed.
var ErrInvalidRecord = errors.New("record is malformed")

// ErrPublicKeyMismatch is return when the public key embedded in an IPNS [Record]
// does not match the expected public key.
var ErrPublicKeyMismatch = errors.New("record public key does not match the expected public key")

// ErrPublicKeyNotFound is returned when the public key is not found.
var ErrPublicKeyNotFound = errors.New("public key not found")

// ErrInvalidPublicKey is returned when an IPNS [Record] has an invalid public key,
var ErrInvalidPublicKey = errors.New("public key invalid")

// ErrSignature is returned when an IPNS [Record] fails signature verification.
var ErrSignature = errors.New("signature verification failed")

// ErrInvalidName is returned when an IPNS [Name] is invalid.
var ErrInvalidName = errors.New("name is invalid")

// ErrInvalidPath is returned when an IPNS [Record] has an invalid path.
var ErrInvalidPath = errors.New("value is not a valid content path")
