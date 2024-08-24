package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"berty.tech/go-ipfs-log/iface"
	"bytes"
	"sort"
)

// Difference gets the list of values not present in both entries sets.
func Difference(a []iface.IPFSLogEntry, b []iface.IPFSLogEntry) []iface.IPFSLogEntry {
	existing := map[string]bool{}
	processed := map[string]bool{}
	var diff []iface.IPFSLogEntry

	for _, v := range a {
		existing[v.GetHash().String()] = true
	}

	for _, v := range b {
		isInFirst := existing[v.GetHash().String()]
		hasBeenProcessed := processed[v.GetHash().String()]
		if !isInFirst && !hasBeenProcessed {
			diff = append(diff, v)
			processed[v.GetHash().String()] = true
		}
	}

	return diff
}

//func FindTails(entries []*Entry) []*Entry {
//	// Reverse index { next -> entry }
//	reverseIndex := map[string][]*Entry{}
//	// Null index containing entries that have no parents (nexts)
//	nullIndex := []*Entry{}
//	// Hashes for all entries for quick lookups
//	hashes := map[string]bool{}
//	// Hashes of all next entries
//	nexts := []cid.Cid{}
//
//	for _, e := range entries {
//		if len(e.Next) == 0 {
//			nullIndex = append(nullIndex, e)
//		}
//
//		for _, nextE := range e.Next {
//			reverseIndex[nextE.String()] = append(reverseIndex[nextE.String()], e)
//		}
//
//		nexts = append(nexts, e.Next...)
//
//		hashes[e.Hash.String()] = true
//	}
//
//	tails := []*Entry{}
//
//	for _, n := range nexts {
//		if _, ok := hashes[n.String()]; !ok {
//			continue
//		}
//
//		tails = append(tails, reverseIndex[n.String()]...)
//	}
//
//	tails = append(tails, nullIndex...)
//
//	return NewOrderedMapFromEntries(tails).Slice()
//}
//
//func FindTailHashes(entries []*Entry) []string {
//	res := []string{}
//	hashes := map[string]bool{}
//	for _, e := range entries {
//		hashes[e.Hash.String()] = true
//	}
//
//	for _, e := range entries {
//		nextLength := len(e.Next)
//
//		for i := range e.Next {
//			next := e.Next[nextLength-i]
//			if _, ok := hashes[next.String()]; !ok {
//				res = append([]string{e.Hash.String()}, res...)
//			}
//		}
//	}
//
//	return res
//}

// FindHeads search entries heads in an OrderedMap.
func FindHeads(entries iface.IPFSLogOrderedEntries) []iface.IPFSLogEntry {
	if entries == nil {
		return nil
	}

	var result []iface.IPFSLogEntry
	items := map[string]string{}

	for _, k := range entries.Keys() {
		e := entries.UnsafeGet(k)
		for _, n := range e.GetNext() {
			items[n.String()] = e.GetHash().String()
		}
	}

	for _, h := range entries.Keys() {
		e, ok := items[h]
		if ok || e != "" {
			continue
		}

		result = append(result, entries.UnsafeGet(h))
	}

	sort.SliceStable(result, func(a, b int) bool {
		return bytes.Compare(result[a].GetClock().GetID(), result[b].GetClock().GetID()) < 0
	})

	return result
}
