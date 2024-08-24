package jsondiff

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"strconv"
)

type hasher struct {
	mh maphash.Hash
}

func (h *hasher) digest(val interface{}) uint64 {
	h.mh.Reset()
	h.hash(val)

	return h.mh.Sum64()
}

func (h *hasher) hash(i interface{}) {
	switch v := i.(type) {
	case string:
		_, _ = h.mh.WriteString(v)
	case bool:
		_, _ = h.mh.WriteString(strconv.FormatBool(v))
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
		_, _ = h.mh.Write(buf[:])
	case nil:
		_ = h.mh.WriteByte('0')
	case []interface{}:
		for i, e := range v {
			_, _ = h.mh.WriteString(strconv.Itoa(i))
			h.hash(e)
		}
	case map[string]interface{}:
		keys := make([]string, 0, len(v))

		// Extract keys first, and sort them
		// in lexicographical order.
		for k := range v {
			keys = append(keys, k)
		}
		sortStrings(keys)

		for _, k := range keys {
			_, _ = h.mh.WriteString(k)
			h.hash(v[k])
		}
	}
}
