package cidutil

import (
	"slices"
	"strings"

	"github.com/ipfs/go-cid"
)

// Slice is a convenience type for sorting CIDs
type Slice []cid.Cid

func (s Slice) Len() int {
	return len(s)
}

func (s Slice) Less(i, j int) bool {
	return s[i].KeyString() < s[j].KeyString()
}

func (s Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Slice) Sort() {
	Sort(s)
}

// Sort sorts a slice of CIDs
func Sort(s []cid.Cid) {
	slices.SortFunc(s, func(a, b cid.Cid) int {
		return strings.Compare(a.KeyString(), b.KeyString())
	})
}
