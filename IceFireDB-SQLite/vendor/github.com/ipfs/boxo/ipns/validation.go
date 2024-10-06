package ipns

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	record "github.com/libp2p/go-libp2p-record"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"google.golang.org/protobuf/proto"
)

// ValidateWithName validates the given IPNS [Record] against the given [Name].
func ValidateWithName(rec *Record, name Name) error {
	pk, err := ExtractPublicKey(rec, name)
	if err != nil {
		return err
	}

	return Validate(rec, pk)
}

// Validates validates the given IPNS Record against the given [crypto.PubKey],
// following the [Record Verification] specification.
//
// [Record Verification]: https://specs.ipfs.tech/ipns/ipns-record/#record-verification
func Validate(rec *Record, pk ic.PubKey) error {
	// (1) Ensure size is not over maximum record size.
	if proto.Size(rec.pb) > MaxRecordSize {
		return ErrRecordSize
	}

	// (2) Ensure SignatureV2 and Data are present and not empty.
	if len(rec.pb.GetSignatureV2()) == 0 {
		return ErrSignature
	}
	if len(rec.pb.GetData()) == 0 {
		return ErrInvalidRecord
	}

	// (3) Extract Public Key - not necessary. Done via [ValidateWithName].

	// (4) Get deserialized Data as DAG-CBOR document.
	sig2Data, err := recordDataForSignatureV2(rec.pb.GetData())
	if err != nil {
		return fmt.Errorf("could not compute signature data: %w", err)
	}

	// (6) Verify signature against concatenation.
	if ok, err := pk.Verify(sig2Data, rec.pb.GetSignatureV2()); err != nil || !ok {
		return ErrSignature
	}

	// (5) Ensure that CBOR data matches Protobuf, only if non-CBOR Value or SignatureV1 are present.
	if len(rec.pb.GetSignatureV1()) != 0 || len(rec.pb.GetValue()) != 0 {
		if err := validateCborDataMatchesPbData(rec.pb); err != nil {
			return err
		}
	}

	// Check EOL.
	eol, err := rec.Validity()
	if err != nil {
		return err
	}

	if time.Now().After(eol) {
		return ErrExpiredRecord
	}

	return nil
}

// TODO: Most of this function could probably be replaced with codegen
func validateCborDataMatchesPbData(entry *ipns_pb.IpnsRecord) error {
	if len(entry.GetData()) == 0 {
		return errors.New("record data is missing")
	}

	ndbuilder := basicnode.Prototype__Map{}.NewBuilder()
	if err := dagcbor.Decode(ndbuilder, bytes.NewReader(entry.GetData())); err != nil {
		return err
	}

	fullNd := ndbuilder.Build()
	nd, err := fullNd.LookupByString(cborValueKey)
	if err != nil {
		return err
	}
	ndBytes, err := nd.AsBytes()
	if err != nil {
		return err
	}
	if !bytes.Equal(entry.GetValue(), ndBytes) {
		return fmt.Errorf("field \"%v\" did not match between protobuf and CBOR", cborValueKey)
	}

	nd, err = fullNd.LookupByString(cborValidityKey)
	if err != nil {
		return err
	}
	ndBytes, err = nd.AsBytes()
	if err != nil {
		return err
	}
	if !bytes.Equal(entry.GetValidity(), ndBytes) {
		return fmt.Errorf("field \"%v\" did not match between protobuf and CBOR", cborValidityKey)
	}

	nd, err = fullNd.LookupByString(cborValidityTypeKey)
	if err != nil {
		return err
	}
	ndInt, err := nd.AsInt()
	if err != nil {
		return err
	}
	if int64(entry.GetValidityType()) != ndInt {
		return fmt.Errorf("field \"%v\" did not match between protobuf and CBOR", cborValidityTypeKey)
	}

	nd, err = fullNd.LookupByString(cborSequenceKey)
	if err != nil {
		return err
	}
	ndInt, err = nd.AsInt()
	if err != nil {
		return err
	}

	if entry.GetSequence() != uint64(ndInt) {
		return fmt.Errorf("field \"%v\" did not match between protobuf and CBOR", cborSequenceKey)
	}

	nd, err = fullNd.LookupByString("TTL")
	if err != nil {
		return err
	}
	ndInt, err = nd.AsInt()
	if err != nil {
		return err
	}
	if entry.GetTtl() != uint64(ndInt) {
		return fmt.Errorf("field \"%v\" did not match between protobuf and CBOR", cborTTLKey)
	}

	return nil
}

var _ record.Validator = Validator{}

// Validator is an IPNS Record validator that satisfies the [record.Validator]
// interface from Libp2p.
type Validator struct {
	// KeyBook, if non-nil, is used to lookup keys for validating IPNS Records.
	KeyBook peerstore.KeyBook
}

// Validate validates an IPNS record.
func (v Validator) Validate(key string, value []byte) error {
	name, err := NameFromRoutingKey([]byte(key))
	if err != nil {
		log.Debugf("failed to parse ipns routing key %q into name", key)
		return ErrInvalidName
	}

	r, err := UnmarshalRecord(value)
	if err != nil {
		return err
	}

	pk, err := v.getPublicKey(r, name)
	if err != nil {
		return err
	}

	return Validate(r, pk)
}

func (v Validator) getPublicKey(r *Record, name Name) (ic.PubKey, error) {
	switch pk, err := ExtractPublicKey(r, name); err {
	case peer.ErrNoPublicKey:
	case nil:
		return pk, nil
	default:
		return nil, err
	}

	if v.KeyBook == nil {
		log.Debugf("public key with hash %q not found in IPNS record and no peer store provided", name.Peer())
		return nil, ErrPublicKeyNotFound
	}

	pk := v.KeyBook.PubKey(name.Peer())
	if pk == nil {
		log.Debugf("public key with hash %q not found in peer store", name.Peer())
		return nil, ErrPublicKeyNotFound
	}

	return pk, nil
}

// Select selects the best record by checking which has the highest sequence
// number and latest validity. This function returns an error if any of the
// records fail to parse.
//
// This function does not validate the records. The caller is responsible for
// ensuring that the Records are valid by using [Validate].
func (v Validator) Select(k string, vals [][]byte) (int, error) {
	var recs []*Record
	for _, v := range vals {
		r, err := UnmarshalRecord(v)
		if err != nil {
			return -1, err
		}
		recs = append(recs, r)
	}

	return selectRecord(recs, vals)
}

func selectRecord(recs []*Record, vals [][]byte) (int, error) {
	switch len(recs) {
	case 0:
		return -1, errors.New("no usable records in given set")
	case 1:
		return 0, nil
	}

	var i int
	for j := 1; j < len(recs); j++ {
		cmp, err := compare(recs[i], recs[j])
		if err != nil {
			return -1, err
		}
		if cmp == 0 {
			cmp = bytes.Compare(vals[i], vals[j])
		}
		if cmp < 0 {
			i = j
		}
	}

	return i, nil
}
