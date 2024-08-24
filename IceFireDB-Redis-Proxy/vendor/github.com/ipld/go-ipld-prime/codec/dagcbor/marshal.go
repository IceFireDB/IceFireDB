package dagcbor

import (
	"fmt"

	"github.com/polydawn/refmt/shared"
	"github.com/polydawn/refmt/tok"

	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// This should be identical to the general feature in the parent package,
// except for the `case ipld.Kind_Link` block,
// which is dag-cbor's special sauce for schemafree links.
func Marshal(n ipld.Node, sink shared.TokenSink) error {
	var tk tok.Token
	return marshal(n, &tk, sink)
}

func marshal(n ipld.Node, tk *tok.Token, sink shared.TokenSink) error {
	switch n.Kind() {
	case ipld.Kind_Invalid:
		return fmt.Errorf("cannot traverse a node that is absent")
	case ipld.Kind_Null:
		tk.Type = tok.TNull
		_, err := sink.Step(tk)
		return err
	case ipld.Kind_Map:
		// Emit start of map.
		tk.Type = tok.TMapOpen
		tk.Length = int(n.Length()) // TODO: overflow check
		if _, err := sink.Step(tk); err != nil {
			return err
		}
		// Emit map contents (and recurse).
		for itr := n.MapIterator(); !itr.Done(); {
			k, v, err := itr.Next()
			if err != nil {
				return err
			}
			tk.Type = tok.TString
			tk.Str, err = k.AsString()
			if err != nil {
				return err
			}
			if _, err := sink.Step(tk); err != nil {
				return err
			}
			if err := marshal(v, tk, sink); err != nil {
				return err
			}
		}
		// Emit map close.
		tk.Type = tok.TMapClose
		_, err := sink.Step(tk)
		return err
	case ipld.Kind_List:
		// Emit start of list.
		tk.Type = tok.TArrOpen
		l := n.Length()
		tk.Length = int(l) // TODO: overflow check
		if _, err := sink.Step(tk); err != nil {
			return err
		}
		// Emit list contents (and recurse).
		for i := int64(0); i < l; i++ {
			v, err := n.LookupByIndex(i)
			if err != nil {
				return err
			}
			if err := marshal(v, tk, sink); err != nil {
				return err
			}
		}
		// Emit list close.
		tk.Type = tok.TArrClose
		_, err := sink.Step(tk)
		return err
	case ipld.Kind_Bool:
		v, err := n.AsBool()
		if err != nil {
			return err
		}
		tk.Type = tok.TBool
		tk.Bool = v
		_, err = sink.Step(tk)
		return err
	case ipld.Kind_Int:
		v, err := n.AsInt()
		if err != nil {
			return err
		}
		tk.Type = tok.TInt
		tk.Int = int64(v)
		_, err = sink.Step(tk)
		return err
	case ipld.Kind_Float:
		v, err := n.AsFloat()
		if err != nil {
			return err
		}
		tk.Type = tok.TFloat64
		tk.Float64 = v
		_, err = sink.Step(tk)
		return err
	case ipld.Kind_String:
		v, err := n.AsString()
		if err != nil {
			return err
		}
		tk.Type = tok.TString
		tk.Str = v
		_, err = sink.Step(tk)
		return err
	case ipld.Kind_Bytes:
		v, err := n.AsBytes()
		if err != nil {
			return err
		}
		tk.Type = tok.TBytes
		tk.Bytes = v
		_, err = sink.Step(tk)
		return err
	case ipld.Kind_Link:
		v, err := n.AsLink()
		if err != nil {
			return err
		}
		switch lnk := v.(type) {
		case cidlink.Link:
			tk.Type = tok.TBytes
			tk.Bytes = append([]byte{0}, lnk.Bytes()...)
			tk.Tagged = true
			tk.Tag = linkTag
			_, err = sink.Step(tk)
			tk.Tagged = false
			return err
		default:
			return fmt.Errorf("schemafree link emission only supported by this codec for CID type links")
		}
	default:
		panic("unreachable")
	}
}
