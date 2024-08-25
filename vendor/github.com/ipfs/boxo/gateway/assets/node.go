package assets

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type ParsedNode struct {
	Keys   []*ParsedNode
	Values []*ParsedNode
	Value  string
	CID    string
	Long   bool
}

func ParseNode(node datamodel.Node) (*ParsedNode, error) {
	dag := &ParsedNode{}

	switch node.Kind() {
	case datamodel.Kind_Map:
		it := node.MapIterator()

		for !it.Done() {
			k, v, err := it.Next()
			if err != nil {
				return nil, err
			}

			kd, err := ParseNode(k)
			if err != nil {
				return nil, err
			}

			vd, err := ParseNode(v)
			if err != nil {
				return nil, err
			}

			dag.Keys = append(dag.Keys, kd)
			dag.Values = append(dag.Values, vd)
		}
	case datamodel.Kind_List:
		it := node.ListIterator()
		for !it.Done() {
			k, v, err := it.Next()
			if err != nil {
				return nil, err
			}

			vd, err := ParseNode(v)
			if err != nil {
				return nil, err
			}

			dag.Keys = append(dag.Keys, &ParsedNode{Value: strconv.FormatInt(k, 10)})
			dag.Values = append(dag.Values, vd)
		}
	case datamodel.Kind_Bool:
		v, err := node.AsBool()
		if err != nil {
			return nil, err
		}
		dag.Value = strconv.FormatBool(v)
	case datamodel.Kind_Int:
		v, err := node.AsInt()
		if err != nil {
			return nil, err
		}
		dag.Value = strconv.FormatInt(v, 10)
	case datamodel.Kind_Float:
		v, err := node.AsFloat()
		if err != nil {
			return nil, err
		}
		dag.Value = fmt.Sprintf("%f", v)
	case datamodel.Kind_String:
		v, err := node.AsString()
		if err != nil {
			return nil, err
		}
		dag.Value = v
	case datamodel.Kind_Bytes:
		v, err := node.AsBytes()
		if err != nil {
			return nil, err
		}
		dag.Long = true
		dag.Value = hex.Dump(v)
	case datamodel.Kind_Link:
		lnk, err := node.AsLink()
		if err != nil {
			return nil, err
		}
		dag.Value = lnk.String()

		cl, isCid := lnk.(cidlink.Link)
		if isCid {
			dag.CID = cl.Cid.String()
		}
	case datamodel.Kind_Invalid:
		dag.Value = "INVALID"
	case datamodel.Kind_Null:
		dag.Value = "NULL"
	default:
		dag.Value = "UNKNOWN"
	}

	return dag, nil
}
