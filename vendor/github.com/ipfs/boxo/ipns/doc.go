// Package ipns implements IPNS record creation, marshaling, and validation as
// specified in the [IPNS Record specification].
//
// # Records
//
// An IPNS [Record] maps a [Name] to a content path, with a sequence number,
// expiration time, and TTL. Records are signed with the private key
// corresponding to the name.
//
//	record, err := ipns.NewRecord(privateKey, path, seq, eol, ttl)
//	if err != nil {
//	    // handle error
//	}
//
//	data, err := ipns.MarshalRecord(record)
//
// Records can be deserialized and validated:
//
//	record, err := ipns.UnmarshalRecord(data)
//	err = ipns.ValidateWithName(record, name)
//
// # Metadata
//
// Records support custom metadata as additional keys in the signed DAG-CBOR
// data, as described in the [Extensible Data] section of the spec. Keys should
// be prefixed with "_" to avoid collisions with future standard fields.
//
//	record, err := ipns.NewRecord(privateKey, path, seq, eol, ttl,
//	    ipns.WithMetadata(map[string]any{
//	        "_myapp_version": "1.0",
//	        "_myapp_flags":   int64(42),
//	    }),
//	)
//
// Metadata can be read back from unmarshaled records:
//
//	mv, err := record.Metadata("_myapp_version")
//	val, err := mv.AsString()
//
// # Names
//
// A [Name] is a [Multihash] of a serialized public key. Names can be created
// from peer IDs or strings:
//
//	name := ipns.NameFromPeer(peerID)
//	name, err := ipns.NameFromString("k51...")
//
// [IPNS Record specification]: https://specs.ipfs.tech/ipns/ipns-record/
// [Extensible Data]: https://specs.ipfs.tech/ipns/ipns-record/#extensible-data-dag-cbor
// [Multihash]: https://specs.ipfs.tech/ipns/ipns-record/#ipns-name
package ipns
