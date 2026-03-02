package dagcbor

import (
	"errors"
	"fmt"
	"io"
	"math"

	cid "github.com/ipfs/go-cid"
	"github.com/polydawn/refmt/cbor"
	"github.com/polydawn/refmt/shared"
	"github.com/polydawn/refmt/tok"

	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var (
	ErrInvalidMultibase         = errors.New("invalid multibase on IPLD link")
	ErrAllocationBudgetExceeded = errors.New("message structure demanded too many resources to process")
	ErrTrailingBytes            = errors.New("unexpected content after end of cbor object")
)

const (
	mapEntryCost  = 8
	listEntryCost = 4
)

// This file should be identical to the general feature in the parent package,
// except for the `case tok.TBytes` block,
// which has dag-cbor's special sauce for detecting schemafree links.

// DecodeOptions can be used to customize the behavior of a decoding function.
// The Decode method on this struct fits the codec.Decoder function interface.
type DecodeOptions struct {
	// If true, parse DAG-CBOR tag(42) as Link nodes, otherwise reject them
	AllowLinks bool

	// TODO: ExperimentalDeterminism enforces map key order, but not the other parts
	// of the spec such as integers or floats. See the fuzz failures spotted in
	// https://github.com/ipld/go-ipld-prime/pull/389.
	// When we're done implementing strictness, deprecate the option in favor of
	// StrictDeterminism, but keep accepting both for backwards compatibility.

	// ExperimentalDeterminism requires decoded DAG-CBOR bytes to be canonical as per
	// the spec. For example, this means that integers and floats be encoded in
	// a particular way, and map keys be sorted.
	//
	// The decoder does not enforce this requirement by default, as the codec
	// was originally implemented without these rules. Because of that, there's
	// a significant amount of published data that isn't canonical but should
	// still decode with the default settings for backwards compatibility.
	//
	// Note that this option is experimental as it only implements partial strictness.
	ExperimentalDeterminism bool

	// If true, the decoder stops reading from the stream at the end of a full,
	// valid CBOR object. This may be useful for parsing a stream of undelimited
	// CBOR objects.
	// As per standard IPLD behavior, in the default mode the parser considers the
	// entire block to be part of the CBOR object and will error if there is
	// extraneous data after the end of the object.
	DontParseBeyondEnd bool

	// AllowBudget sets the maximum budget for the decoder. The budget is
	// decremented as the decoder allocates resources (nodes, map entries, list
	// elements, string/bytes content). If the budget is exhausted, the decoder
	// returns ErrAllocationBudgetExceeded.
	//
	// When zero, a default budget is used which is generous for typical IPLD
	// block sizes.
	AllocationBudget int64

	// MaxCollectionPrealloc sets the maximum size hint passed to
	// BeginMap/BeginList. CBOR headers declare collection sizes upfront;
	// this caps the initial allocation while collections grow dynamically
	// as entries are decoded.
	//
	// When zero, a default of 1024 is used.
	MaxCollectionPrealloc int64
}

const (
	defaultAllocationBudget      int64 = 1048576 * 10
	defaultMaxCollectionPrealloc int64 = 1024
)

// Decode deserializes data from the given io.Reader and feeds it into the given datamodel.NodeAssembler.
// Decode fits the codec.Decoder function interface.
//
// The behavior of the decoder can be customized by setting fields in the DecodeOptions struct before calling this method.
func (cfg DecodeOptions) Decode(na datamodel.NodeAssembler, r io.Reader) error {
	// Probe for a builtin fast path.  Shortcut to that if possible.
	type detectFastPath interface {
		DecodeDagCbor(io.Reader) error
	}
	if na2, ok := na.(detectFastPath); ok {
		return na2.DecodeDagCbor(r)
	}
	// Okay, generic builder path.
	err := Unmarshal(na, cbor.NewDecoder(cbor.DecodeOptions{
		CoerceUndefToNull: true,
	}, r), cfg)

	if err != nil {
		return err
	}

	if cfg.DontParseBeyondEnd {
		return nil
	}

	var buf [1]byte
	_, err = io.ReadFull(r, buf[:])
	switch err {
	case io.EOF:
		return nil
	case nil:
		return ErrTrailingBytes
	default:
		return err
	}
}

// Future work: we would like to remove the Unmarshal function,
// and in particular, stop seeing types from refmt (like shared.TokenSource) be visible.
// Right now, some kinds of configuration (e.g. for whitespace and prettyprint) are only available through interacting with the refmt types;
// we should improve our API so that this can be done with only our own types in this package.

// Unmarshal is a deprecated function.
// Please consider switching to DecodeOptions.Decode instead.
func Unmarshal(na datamodel.NodeAssembler, tokSrc shared.TokenSource, options DecodeOptions) error {
	budget := options.AllocationBudget
	if budget == 0 {
		budget = defaultAllocationBudget
	}
	return unmarshal1(na, tokSrc, &budget, options)
}

func (cfg DecodeOptions) maxPrealloc() int64 {
	if cfg.MaxCollectionPrealloc > 0 {
		return cfg.MaxCollectionPrealloc
	}
	return defaultMaxCollectionPrealloc
}

func unmarshal1(na datamodel.NodeAssembler, tokSrc shared.TokenSource, budget *int64, options DecodeOptions) error {
	var tk tok.Token
	done, err := tokSrc.Step(&tk)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	if err != nil {
		return err
	}
	if done && !tk.Type.IsValue() && tk.Type != tok.TNull {
		return fmt.Errorf("unexpected eof")
	}
	return unmarshal2(na, tokSrc, &tk, budget, options)
}

// starts with the first token already primed.  Necessary to get recursion
//
//	to flow right without a peek+unpeek system.
func unmarshal2(na datamodel.NodeAssembler, tokSrc shared.TokenSource, tk *tok.Token, budget *int64, options DecodeOptions) error {
	// FUTURE: check for schema.TypedNodeBuilder that's going to parse a Link (they can slurp any token kind they want).
	switch tk.Type {
	case tok.TMapOpen:
		expectLen := int64(tk.Length)
		allocLen := int64(tk.Length)
		if tk.Length == -1 {
			expectLen = math.MaxInt64
			allocLen = 0
		} else {
			*budget -= allocLen
			if *budget < 0 {
				return ErrAllocationBudgetExceeded
			}
			if allocLen > options.maxPrealloc() {
				allocLen = options.maxPrealloc()
			}
		}
		ma, err := na.BeginMap(allocLen)
		if err != nil {
			return err
		}
		var observedLen int64
		lastKey := ""
		for {
			_, err := tokSrc.Step(tk)
			if err != nil {
				return err
			}
			switch tk.Type {
			case tok.TMapClose:
				if expectLen != math.MaxInt64 && observedLen != expectLen {
					return fmt.Errorf("unexpected mapClose before declared length")
				}
				return ma.Finish()
			case tok.TString:
				*budget -= int64(len(tk.Str) + mapEntryCost)
				if *budget < 0 {
					return ErrAllocationBudgetExceeded
				}
				// continue
			default:
				return fmt.Errorf("unexpected %s token while expecting map key", tk.Type)
			}
			observedLen++
			if observedLen > expectLen {
				return fmt.Errorf("unexpected continuation of map elements beyond declared length")
			}
			if observedLen > 1 && options.ExperimentalDeterminism {
				if len(lastKey) > len(tk.Str) || lastKey > tk.Str {
					return fmt.Errorf("map key %q is not after %q as per RFC7049", tk.Str, lastKey)
				}
			}
			lastKey = tk.Str
			mva, err := ma.AssembleEntry(tk.Str)
			if err != nil { // return in error if the key was rejected
				return err
			}
			err = unmarshal1(mva, tokSrc, budget, options)
			if err != nil { // return in error if some part of the recursion errored
				return err
			}
		}
	case tok.TMapClose:
		return fmt.Errorf("unexpected mapClose token")
	case tok.TArrOpen:
		expectLen := int64(tk.Length)
		allocLen := int64(tk.Length)
		if tk.Length == -1 {
			expectLen = math.MaxInt64
			allocLen = 0
		} else {
			*budget -= allocLen
			if *budget < 0 {
				return ErrAllocationBudgetExceeded
			}
			if allocLen > options.maxPrealloc() {
				allocLen = options.maxPrealloc()
			}
		}
		la, err := na.BeginList(allocLen)
		if err != nil {
			return err
		}
		var observedLen int64
		for {
			_, err := tokSrc.Step(tk)
			if err != nil {
				return err
			}
			switch tk.Type {
			case tok.TArrClose:
				if expectLen != math.MaxInt64 && observedLen != expectLen {
					return fmt.Errorf("unexpected arrClose before declared length")
				}
				return la.Finish()
			default:
				*budget -= listEntryCost
				if *budget < 0 {
					return ErrAllocationBudgetExceeded
				}
				observedLen++
				if observedLen > expectLen {
					return fmt.Errorf("unexpected continuation of array elements beyond declared length")
				}
				err := unmarshal2(la.AssembleValue(), tokSrc, tk, budget, options)
				if err != nil { // return in error if some part of the recursion errored
					return err
				}
			}
		}
	case tok.TArrClose:
		return fmt.Errorf("unexpected arrClose token")
	case tok.TNull:
		return na.AssignNull()
	case tok.TString:
		*budget -= int64(len(tk.Str))
		if *budget < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignString(tk.Str)
	case tok.TBytes:
		*budget -= int64(len(tk.Bytes))
		if *budget < 0 {
			return ErrAllocationBudgetExceeded
		}
		if !tk.Tagged {
			return na.AssignBytes(tk.Bytes)
		}
		switch tk.Tag {
		case linkTag:
			if !options.AllowLinks {
				return fmt.Errorf("unhandled cbor tag %d", tk.Tag)
			}
			if len(tk.Bytes) < 1 || tk.Bytes[0] != 0 {
				return ErrInvalidMultibase
			}
			elCid, err := cid.Cast(tk.Bytes[1:])
			if err != nil {
				return err
			}
			return na.AssignLink(cidlink.Link{Cid: elCid})
		default:
			return fmt.Errorf("unhandled cbor tag %d", tk.Tag)
		}
	case tok.TBool:
		*budget -= 1
		if *budget < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignBool(tk.Bool)
	case tok.TInt:
		*budget -= 1
		if *budget < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignInt(tk.Int)
	case tok.TUint:
		*budget -= 1
		if *budget < 0 {
			return ErrAllocationBudgetExceeded
		}
		// note that this pushes any overflow errors up the stack when AsInt() may
		// be called on a UintNode that is too large to cast to an int64
		if tk.Uint > math.MaxInt64 {
			return na.AssignNode(basicnode.NewUint(tk.Uint))
		}
		return na.AssignInt(int64(tk.Uint))
	case tok.TFloat64:
		*budget -= 1
		if *budget < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignFloat(tk.Float64)
	default:
		panic("unreachable")
	}
}
