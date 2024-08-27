//go:generate protoc -I=pb --go_out=pb pb/record.proto
package ipns

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

var log = logging.Logger("ipns")

type ValidityType int64

// ValidityEOL means "this record is valid until {Validity}". This is currently
// the only supported Validity type.
const ValidityEOL ValidityType = 0

// Record represents an [IPNS Record].
//
// [IPNS Record]: https://specs.ipfs.tech/ipns/ipns-record/
type Record struct {
	pb   *ipns_pb.IpnsRecord
	node datamodel.Node
}

// UnmarshalRecord parses the [Protobuf-serialized] IPNS Record into a usable
// [Record] struct. Please note that this function does not perform a full
// validation of the record. For that use [Validate].
//
// [Protobuf-serialized]: https://specs.ipfs.tech/ipns/ipns-record/#record-serialization-format
func UnmarshalRecord(data []byte) (*Record, error) {
	if len(data) > MaxRecordSize {
		return nil, ErrRecordSize
	}

	var pb ipns_pb.IpnsRecord
	err := proto.Unmarshal(data, &pb)
	if err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}

	record := &Record{
		pb: &pb,
	}

	// Ensure the record has DAG-CBOR data because we need it.
	if len(pb.GetData()) == 0 {
		return nil, multierr.Combine(ErrInvalidRecord, ErrDataMissing)
	}

	// Decode CBOR data.
	builder := basicnode.Prototype__Map{}.NewBuilder()
	if err := dagcbor.Decode(builder, bytes.NewReader(pb.GetData())); err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}
	record.node = builder.Build()

	return record, nil
}

// MarshalRecord encodes the given IPNS Record into its [Protobuf serialization format].
//
// [Protobuf serialization format]: https://specs.ipfs.tech/ipns/ipns-record/#record-serialization-format
func MarshalRecord(rec *Record) ([]byte, error) {
	return proto.Marshal(rec.pb)
}

// Value returns the [path.Path] that is embedded in this IPNS Record. If the
// path is invalid, an [ErrInvalidPath] is returned.
func (rec *Record) Value() (path.Path, error) {
	value, err := rec.getBytesValue(cborValueKey)
	if err != nil {
		return nil, err
	}

	p, err := path.NewPath(string(value))
	if err != nil {
		return nil, multierr.Combine(ErrInvalidPath, err)
	}

	return p, nil
}

func (rec *Record) ValidityType() (ValidityType, error) {
	value, err := rec.getIntValue(cborValidityTypeKey)
	if err != nil {
		return -1, err
	}

	return ValidityType(value), nil
}

// Validity returns the validity of the IPNS Record. This function returns
// [ErrUnrecognizedValidity] if the validity type of the record isn't EOL.
// Otherwise, it returns an error if it can't parse the EOL.
func (rec *Record) Validity() (time.Time, error) {
	validityType, err := rec.ValidityType()
	if err != nil {
		return time.Time{}, err
	}

	switch validityType {
	case ValidityEOL:
		value, err := rec.getBytesValue(cborValidityKey)
		if err != nil {
			return time.Time{}, err
		}

		v, err := util.ParseRFC3339(string(value))
		if err != nil {
			return time.Time{}, multierr.Combine(ErrInvalidValidity, err)
		}
		return v, nil
	default:
		return time.Time{}, ErrUnrecognizedValidity
	}
}

func (rec *Record) Sequence() (uint64, error) {
	value, err := rec.getIntValue(cborSequenceKey)
	if err != nil {
		return 0, err
	}

	return uint64(value), nil
}

func (rec *Record) TTL() (time.Duration, error) {
	value, err := rec.getIntValue(cborTTLKey)
	if err != nil {
		return 0, err
	}

	return time.Duration(value), nil
}

func (rec *Record) PubKey() (ic.PubKey, error) {
	if pk := rec.pb.GetPubKey(); len(pk) != 0 {
		return ic.UnmarshalPublicKey(pk)
	}

	return nil, ErrPublicKeyNotFound
}

func (rec *Record) getBytesValue(key string) ([]byte, error) {
	node, err := rec.node.LookupByString(key)
	if err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}

	value, err := node.AsBytes()
	if err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}

	return value, nil
}

func (rec *Record) getIntValue(key string) (int64, error) {
	node, err := rec.node.LookupByString(key)
	if err != nil {
		return -1, multierr.Combine(ErrInvalidRecord, err)
	}

	value, err := node.AsInt()
	if err != nil {
		return -1, multierr.Combine(ErrInvalidRecord, err)
	}

	return value, nil
}

const (
	cborValidityKey     = "Validity"
	cborValidityTypeKey = "ValidityType"
	cborValueKey        = "Value"
	cborSequenceKey     = "Sequence"
	cborTTLKey          = "TTL"
)

type options struct {
	v1Compatibility bool
	embedPublicKey  *bool
}

type Option func(*options)

func WithV1Compatibility(compatible bool) Option {
	return func(o *options) {
		o.v1Compatibility = compatible
	}
}

func WithPublicKey(embedded bool) Option {
	return func(o *options) {
		o.embedPublicKey = &embedded
	}
}

func processOptions(opts ...Option) *options {
	options := &options{
		// TODO: produce V2-only records by default after IPIP-XXXX ships with Kubo
		// and Helia for at least 6 months.
		v1Compatibility: true,
	}

	for _, opt := range opts {
		opt(options)
	}
	return options
}

// NewRecord creates a new IPNS [Record] and signs it with the given private key.
// By default, we embed the public key for key types whose peer IDs do not encode
// the public key, such as RSA and ECDSA key types. This can be changed with the
// option [WithPublicKey]. In addition, records are, by default created with V1
// compatibility.
func NewRecord(sk ic.PrivKey, value path.Path, seq uint64, eol time.Time, ttl time.Duration, opts ...Option) (*Record, error) {
	options := processOptions(opts...)

	node, err := createNode(value, seq, eol, ttl)
	if err != nil {
		return nil, err
	}

	cborData, err := nodeToCBOR(node)
	if err != nil {
		return nil, err
	}

	sig2Data, err := recordDataForSignatureV2(cborData)
	if err != nil {
		return nil, err
	}

	sig2, err := sk.Sign(sig2Data)
	if err != nil {
		return nil, err
	}

	pb := ipns_pb.IpnsRecord{
		Data:        cborData,
		SignatureV2: sig2,
	}

	if options.v1Compatibility {
		pb.Value = []byte(value.String())
		typ := ipns_pb.IpnsRecord_EOL
		pb.ValidityType = &typ
		pb.Sequence = &seq
		pb.Validity = []byte(util.FormatRFC3339(eol))
		ttlNs := uint64(ttl.Nanoseconds())
		pb.Ttl = proto.Uint64(ttlNs)

		// For now we still create V1 signatures. These are deprecated, and not
		// used during verification anymore (Validate func requires SignatureV2),
		// but setting it here allows legacy nodes (e.g., go-ipfs < v0.9.0) to
		// still resolve IPNS published by modern nodes.
		sig1, err := sk.Sign(recordDataForSignatureV1(&pb))
		if err != nil {
			return nil, fmt.Errorf("%w: could not compute signature data", err)
		}
		pb.SignatureV1 = sig1
	}

	embedPublicKey := false
	if options.embedPublicKey == nil {
		embedPublicKey, err = needToEmbedPublicKey(sk.GetPublic())
		if err != nil {
			return nil, err
		}
	} else {
		embedPublicKey = *options.embedPublicKey
	}

	if embedPublicKey {
		pkBytes, err := ic.MarshalPublicKey(sk.GetPublic())
		if err != nil {
			return nil, err
		}
		pb.PubKey = pkBytes
	}

	return &Record{
		pb:   &pb,
		node: node,
	}, nil
}

func createNode(value path.Path, seq uint64, eol time.Time, ttl time.Duration) (datamodel.Node, error) {
	m := make(map[string]ipld.Node)
	var keys []string

	m[cborValueKey] = basicnode.NewBytes([]byte(value.String()))
	keys = append(keys, cborValueKey)

	m[cborValidityKey] = basicnode.NewBytes([]byte(util.FormatRFC3339(eol)))
	keys = append(keys, cborValidityKey)

	m[cborValidityTypeKey] = basicnode.NewInt(int64(ValidityEOL))
	keys = append(keys, cborValidityTypeKey)

	m[cborSequenceKey] = basicnode.NewInt(int64(seq))
	keys = append(keys, cborSequenceKey)

	m[cborTTLKey] = basicnode.NewInt(int64(ttl))
	keys = append(keys, cborTTLKey)

	sort.Slice(keys, func(i, j int) bool {
		li, lj := len(keys[i]), len(keys[j])
		if li == lj {
			return keys[i] < keys[j]
		}
		return li < lj
	})

	newNd := basicnode.Prototype__Map{}.NewBuilder()
	ma, err := newNd.BeginMap(int64(len(keys)))
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		if err := ma.AssembleKey().AssignString(k); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignNode(m[k]); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	return newNd.Build(), nil
}

func nodeToCBOR(node datamodel.Node) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := dagcbor.Encode(node, buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func recordDataForSignatureV1(e *ipns_pb.IpnsRecord) []byte {
	return bytes.Join([][]byte{
		e.Value,
		e.Validity,
		[]byte(fmt.Sprint(e.GetValidityType())),
	},
		[]byte{})
}

func recordDataForSignatureV2(data []byte) ([]byte, error) {
	dataForSig := []byte("ipns-signature:")
	dataForSig = append(dataForSig, data...)
	return dataForSig, nil
}

func needToEmbedPublicKey(pk ic.PubKey) (bool, error) {
	// First try extracting the peer ID from the public key.
	pid, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return false, fmt.Errorf("cannot convert public key to peer ID: %w", err)
	}

	_, err = pid.ExtractPublicKey()
	if err == nil {
		// Can be extracted, therefore no need to embed the public key.
		return false, nil
	}

	if errors.Is(err, peer.ErrNoPublicKey) {
		return true, nil
	}

	return false, fmt.Errorf("cannot extract ID from public key: %w", err)
}

// compare compares two IPNS Records. It returns:
//
//   - -1 if a is older than b
//   - 0 if a and b cannot be ordered (this doesn't mean that they are equal)
//   - +1 if a is newer than b
//
// This function does not validate the records. The caller is responsible for
// ensuring that the Records are valid by using [Validate].
func compare(a, b *Record) (int, error) {
	aHasV2Sig := a.pb.GetSignatureV2() != nil
	bHasV2Sig := b.pb.GetSignatureV2() != nil

	// Having a newer signature version is better than an older signature version
	if aHasV2Sig && !bHasV2Sig {
		return 1, nil
	} else if !aHasV2Sig && bHasV2Sig {
		return -1, nil
	}

	as, err := a.Sequence()
	if err != nil {
		return 0, err
	}

	bs, err := b.Sequence()
	if err != nil {
		return 0, err
	}

	if as > bs {
		return 1, nil
	} else if as < bs {
		return -1, nil
	}

	at, err := a.Validity()
	if err != nil {
		return 0, err
	}

	bt, err := b.Validity()
	if err != nil {
		return 0, err
	}

	if at.After(bt) {
		return 1, nil
	} else if bt.After(at) {
		return -1, nil
	}

	return 0, nil
}

// ExtractPublicKey extracts a [crypto.PubKey] matching the given [Name] from
// the IPNS Record, if possible.
func ExtractPublicKey(rec *Record, name Name) (ic.PubKey, error) {
	if pk, err := rec.PubKey(); err == nil {
		expPid, err := peer.IDFromPublicKey(pk)
		if err != nil {
			return nil, multierr.Combine(ErrInvalidPublicKey, err)
		}

		if !name.Equal(NameFromPeer(expPid)) {
			return nil, ErrPublicKeyMismatch
		}

		return pk, nil
	} else if !errors.Is(err, ErrPublicKeyNotFound) {
		return nil, multierr.Combine(ErrInvalidPublicKey, err)
	} else {
		return name.Peer().ExtractPublicKey()
	}
}
