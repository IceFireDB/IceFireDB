// Package errmsg defines error messages used by the Go version of IPFS Log.
package errmsg // import "berty.tech/go-ipfs-log/errmsg"

import (
	"fmt"
)

// https://dave.cheney.net/2016/04/07/constant-errors

type Error string

func (e Error) Error() string { return string(e) }

func (e Error) Wrap(inner error) error { return fmt.Errorf("%s: %w", e, inner) }

const (
	ErrCBOROperationFailed          = Error("CBOR operation failed")
	ErrCIDSerializationFailed       = Error("CID deserialization failed")
	ErrClockDeserialization         = Error("unable to deserialize clock")
	ErrEmptyLogSerialization        = Error("can't serialize an empty log")
	ErrEntriesNotDefined            = Error("entries not defined")
	ErrEntryDeserializationFailed   = Error("entry deserialization failed")
	ErrEntryNotDefined              = Error("entry is not defined")
	ErrEntryNotHashable             = Error("entry is hashable")
	ErrFetchOptionsNotDefined       = Error("fetch options not defined")
	ErrFilterLTENotFound            = Error("entry specified at LTE not found")
	ErrFilterLTNotFound             = Error("entry specified at LT not found")
	ErrIPFSNotDefined               = Error("ipfs instance not defined")
	ErrIPFSOperationFailed          = Error("IPFS operation failed")
	ErrIdentityCreationFailed       = Error("identity creation failed")
	ErrIdentityDeserialization      = Error("unable to deserialize identity")
	ErrIdentityNotDefined           = Error("identity not defined")
	ErrIdentityProviderNotDefined   = Error("an identity provider constructor needs to be given as an option")
	ErrIdentityProviderNotSupported = Error("identity provider is not supported")
	ErrIdentitySigDeserialization   = Error("identity signature deserialization failed")
	ErrIdentityUnknown              = Error("unknown identity used")
	ErrInvalidPrivKeyFormat         = Error("unable to unmarshal private key")
	ErrInvalidPubKeyFormat          = Error("unable to unmarshal public key")
	ErrIteratorOptionsNotDefined    = Error("no iterator options specified")
	ErrJSONSerializationFailed      = Error("JSON serialization failed")
	ErrKeyDeserialization           = Error("unable to deserialize key")
	ErrKeyGenerationFailed          = Error("key generation failed")
	ErrKeyNotDefined                = Error("key is not defined")
	ErrKeyNotInKeystore             = Error("private signing key not found from Keystore")
	ErrKeyStoreCreateEntry          = Error("unable to create key store entry")
	ErrKeyStoreInitFailed           = Error("keystore initialization failed")
	ErrKeyStorePutFailed            = Error("keystore put failed")
	ErrKeystoreNotDefined           = Error("keystore not defined")
	ErrLogAppendDenied              = Error("log append denied")
	ErrLogAppendFailed              = Error("log append failed")
	ErrLogFromEntry                 = Error("new from entry failed")
	ErrLogFromEntryHash             = Error("new from multi hash failed")
	ErrLogFromJSON                  = Error("new from JSON failed")
	ErrLogFromMultiHash             = Error("new from entry hash failed")
	ErrLogIDNotDefined              = Error("log ID not defined")
	ErrLogJoinFailed                = Error("log join failed")
	ErrLogJoinNotDefined            = Error("log to join not defined")
	ErrLogOptionsNotDefined         = Error("log options not defined")
	ErrLogTraverseFailed            = Error("log traverse failed")
	ErrMultibaseOperationFailed     = Error("Multibase operation failed")
	ErrNotSecp256k1PubKey           = Error("supplied key is not a valid Secp256k1 public key")
	ErrOutputChannelNotDefined      = Error("no output channel specified")
	ErrPayloadNotDefined            = Error("payload not defined")
	ErrPubKeyDeserialization        = Error("public key deserialization failed")
	ErrPubKeySerialization          = Error("unable to serialize public key")
	ErrSigDeserialization           = Error("unable to deserialize signature")
	ErrSigNotDefined                = Error("signature is not defined")
	ErrSigNotVerified               = Error("signature could not verified")
	ErrSigSign                      = Error("unable to sign value")
	ErrTiebreakerBogus              = Error("log's tiebreaker function has returned zero and therefore cannot be")
	ErrTiebreakerFailed             = Error("tiebreaker failed")
	ErrIPFSWriteFailed              = Error("ipfs write failed")
	ErrIPFSReadFailed               = Error("ipfs read failed")
	ErrIPFSReadUnmarshalFailed      = Error("ipfs unmarshal failed")
	ErrPBReadUnmarshalFailed        = Error("protobuf unmarshal failed")
	ErrEncrypt                      = Error("encryption error")
	ErrDecrypt                      = Error("decryption error")
)
