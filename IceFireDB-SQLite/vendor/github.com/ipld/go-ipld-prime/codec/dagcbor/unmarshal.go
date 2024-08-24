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
)

var (
	ErrInvalidMultibase         = errors.New("invalid multibase on IPLD link")
	ErrAllocationBudgetExceeded = errors.New("message structure demanded too many resources to process")
)

const (
	mapEntryGasScore  = 8
	listEntryGasScore = 4
)

// This file should be identical to the general feature in the parent package,
// except for the `case tok.TBytes` block,
// which has dag-cbor's special sauce for detecting schemafree links.

// DecodeOptions can be used to customize the behavior of a decoding function.
// The Decode method on this struct fits the codec.Decoder function interface.
type DecodeOptions struct {
	// If true, parse DAG-CBOR tag(42) as Link nodes, otherwise reject them
	AllowLinks bool
}

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
	return Unmarshal(na, cbor.NewDecoder(cbor.DecodeOptions{
		CoerceUndefToNull: true,
	}, r), cfg)
}

// Future work: we would like to remove the Unmarshal function,
// and in particular, stop seeing types from refmt (like shared.TokenSource) be visible.
// Right now, some kinds of configuration (e.g. for whitespace and prettyprint) are only available through interacting with the refmt types;
// we should improve our API so that this can be done with only our own types in this package.

// Unmarshal is a deprecated function.
// Please consider switching to DecodeOptions.Decode instead.
func Unmarshal(na datamodel.NodeAssembler, tokSrc shared.TokenSource, options DecodeOptions) error {
	// Have a gas budget, which will be decremented as we allocate memory, and an error returned when execeeded (or about to be exceeded).
	//  This is a DoS defense mechanism.
	//  It's *roughly* in units of bytes (but only very, VERY roughly) -- it also treats words as 1 in many cases.
	// FUTURE: this ought be configurable somehow.  (How, and at what granularity though?)
	var gas int = 1048576 * 10
	return unmarshal1(na, tokSrc, &gas, options)
}

func unmarshal1(na datamodel.NodeAssembler, tokSrc shared.TokenSource, gas *int, options DecodeOptions) error {
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
	return unmarshal2(na, tokSrc, &tk, gas, options)
}

// starts with the first token already primed.  Necessary to get recursion
//  to flow right without a peek+unpeek system.
func unmarshal2(na datamodel.NodeAssembler, tokSrc shared.TokenSource, tk *tok.Token, gas *int, options DecodeOptions) error {
	// FUTURE: check for schema.TypedNodeBuilder that's going to parse a Link (they can slurp any token kind they want).
	switch tk.Type {
	case tok.TMapOpen:
		expectLen := tk.Length
		allocLen := tk.Length
		if tk.Length == -1 {
			expectLen = math.MaxInt32
			allocLen = 0
		} else {
			if *gas-allocLen < 0 { // halt early if this will clearly demand too many resources
				return ErrAllocationBudgetExceeded
			}
		}
		ma, err := na.BeginMap(int64(allocLen))
		if err != nil {
			return err
		}
		observedLen := 0
		for {
			_, err := tokSrc.Step(tk)
			if err != nil {
				return err
			}
			switch tk.Type {
			case tok.TMapClose:
				if expectLen != math.MaxInt32 && observedLen != expectLen {
					return fmt.Errorf("unexpected mapClose before declared length")
				}
				return ma.Finish()
			case tok.TString:
				*gas -= len(tk.Str) + mapEntryGasScore
				if *gas < 0 {
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
			mva, err := ma.AssembleEntry(tk.Str)
			if err != nil { // return in error if the key was rejected
				return err
			}
			err = unmarshal1(mva, tokSrc, gas, options)
			if err != nil { // return in error if some part of the recursion errored
				return err
			}
		}
	case tok.TMapClose:
		return fmt.Errorf("unexpected mapClose token")
	case tok.TArrOpen:
		expectLen := tk.Length
		allocLen := tk.Length
		if tk.Length == -1 {
			expectLen = math.MaxInt32
			allocLen = 0
		} else {
			if *gas-allocLen < 0 { // halt early if this will clearly demand too many resources
				return ErrAllocationBudgetExceeded
			}
		}
		la, err := na.BeginList(int64(allocLen))
		if err != nil {
			return err
		}
		observedLen := 0
		for {
			_, err := tokSrc.Step(tk)
			if err != nil {
				return err
			}
			switch tk.Type {
			case tok.TArrClose:
				if expectLen != math.MaxInt32 && observedLen != expectLen {
					return fmt.Errorf("unexpected arrClose before declared length")
				}
				return la.Finish()
			default:
				*gas -= listEntryGasScore
				if *gas < 0 {
					return ErrAllocationBudgetExceeded
				}
				observedLen++
				if observedLen > expectLen {
					return fmt.Errorf("unexpected continuation of array elements beyond declared length")
				}
				err := unmarshal2(la.AssembleValue(), tokSrc, tk, gas, options)
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
		*gas -= len(tk.Str)
		if *gas < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignString(tk.Str)
	case tok.TBytes:
		*gas -= len(tk.Bytes)
		if *gas < 0 {
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
		*gas -= 1
		if *gas < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignBool(tk.Bool)
	case tok.TInt:
		*gas -= 1
		if *gas < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignInt(tk.Int)
	case tok.TUint:
		*gas -= 1
		if *gas < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignInt(int64(tk.Uint)) // FIXME overflow check
	case tok.TFloat64:
		*gas -= 1
		if *gas < 0 {
			return ErrAllocationBudgetExceeded
		}
		return na.AssignFloat(tk.Float64)
	default:
		panic("unreachable")
	}
}
