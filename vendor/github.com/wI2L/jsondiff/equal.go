package jsondiff

import (
	"fmt"
	"reflect"
)

// areComparable returns whether the interface values
// i1 and i2 can be compared. The values are comparable
// only if they are both non-nil and share the same kind.
func areComparable(i1, i2 interface{}) bool {
	return typeSwitchKind(i1) == typeSwitchKind(i2)
}

// typeSwitchKind returns the reflect.Kind of
// the interface i using a type switch statement.
func typeSwitchKind(i interface{}) reflect.Kind {
	switch i.(type) {
	case string:
		return reflect.String
	case bool:
		return reflect.Bool
	case float64:
		return reflect.Float64
	case nil:
		return reflect.Ptr
	case []interface{}:
		return reflect.Slice
	case map[string]interface{}:
		return reflect.Map
	default:
		panic(fmt.Sprintf("invalid json type %T", i))
	}
}

func deepEqual(src, tgt interface{}) bool {
	if src == nil || tgt == nil {
		return src == tgt
	}
	srcKind := typeSwitchKind(src)
	if srcKind != typeSwitchKind(tgt) {
		return false
	}
	return deepValueEqual(src, tgt, srcKind)
}

func deepValueEqual(src, tgt interface{}, kind reflect.Kind) bool {
	switch kind {
	case reflect.String:
		return src.(string) == tgt.(string)
	case reflect.Bool:
		return src.(bool) == tgt.(bool)
	case reflect.Float64:
		return src.(float64) == tgt.(float64)
	case reflect.Slice:
		oarr := src.([]interface{})
		narr := tgt.([]interface{})

		if len(oarr) != len(narr) {
			return false
		}
		for i := 0; i < len(oarr); i++ {
			if !deepEqual(oarr[i], narr[i]) {
				return false
			}
		}
		return true
	case reflect.Map:
		oobj := src.(map[string]interface{})
		nobj := tgt.(map[string]interface{})

		if len(oobj) != len(nobj) {
			return false
		}
		for k := range oobj {
			v1 := oobj[k]
			v2, ok := nobj[k]
			if !ok {
				// Key not found in target.
				return false
			}
			if !deepEqual(v1, v2) {
				return false
			}
		}
		return true
	default:
		panic("unknown json type")
	}
}
