// Package sorting includes utilities for ordering slices of Entries.
package sorting

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/iface"
)

func SortByClocks(a, b iface.IPFSLogEntry, resolveConflict func(a iface.IPFSLogEntry, b iface.IPFSLogEntry) (int, error)) (int, error) {
	diff := a.GetClock().Compare(b.GetClock())

	if diff == 0 {
		return resolveConflict(a, b)
	}

	return diff, nil
}

func SortByClockID(a, b iface.IPFSLogEntry, resolveConflict func(a iface.IPFSLogEntry, b iface.IPFSLogEntry) (int, error)) (int, error) {
	comparedIDs := bytes.Compare(a.GetClock().GetID(), b.GetClock().GetID())

	if comparedIDs == 0 {
		return resolveConflict(a, b)
	}

	return comparedIDs, nil
}

func First(_, _ iface.IPFSLogEntry) (int, error) {
	return 1, nil
}

func FirstWriteWins(a, b iface.IPFSLogEntry) (int, error) {
	res, err := LastWriteWins(a, b)

	if err != nil {
		return 0, errmsg.ErrTiebreakerFailed.Wrap(err)
	}

	return res * -1, nil
}

func LastWriteWins(a, b iface.IPFSLogEntry) (int, error) {
	sortByID := func(a, b iface.IPFSLogEntry) (int, error) {
		return SortByClockID(a, b, First)
	}

	sortByEntryClocks := func(a, b iface.IPFSLogEntry) (int, error) {
		return SortByClocks(a, b, sortByID)
	}

	return sortByEntryClocks(a, b)
}

func SortByEntryHash(a, b iface.IPFSLogEntry) (int, error) {
	// Ultimate conflict resolution (compare hashes)
	compareHash := func(a, b iface.IPFSLogEntry) (int, error) {
		return strings.Compare(a.GetHash().String(), b.GetHash().String()), nil
	}

	// Sort two entries by their clock id, if the same then compare hashes
	sortByID := func(a, b iface.IPFSLogEntry) (int, error) {
		return SortByClockID(a, b, compareHash)
	}

	// Sort two entries by their clock time, if concurrent,
	// determine sorting using provided conflict resolution function
	// Sort entries by clock time as the primary sort criteria
	return SortByClocks(a, b, sortByID)
}

func NoZeroes(compFunc func(a, b iface.IPFSLogEntry) (int, error)) func(a, b iface.IPFSLogEntry) (int, error) {
	return func(a, b iface.IPFSLogEntry) (int, error) {
		ret, err := compFunc(a, b)
		if err != nil {
			return 0, errmsg.ErrTiebreakerFailed.Wrap(err)
		}

		if ret != 0 {
			return ret, nil
		}

		return 0, errmsg.ErrTiebreakerBogus
	}
}

func Reverse(a []iface.IPFSLogEntry) {
	for i := len(a)/2 - 1; i >= 0; i-- {
		opp := len(a) - 1 - i
		a[i], a[opp] = a[opp], a[i]
	}
}

func Compare(a, b iface.IPFSLogEntry) (int, error) {
	// TODO: Make it a Golang slice-compatible sort function
	if a == nil || b == nil || !a.Defined() || !b.Defined() {
		return 0, errmsg.ErrEntryNotDefined
	}

	return a.GetClock().Compare(b.GetClock()), nil
}

func Sort(compFunc func(a, b iface.IPFSLogEntry) (int, error), values []iface.IPFSLogEntry, reverse bool) {
	if reverse {
		sort.SliceStable(values, func(i, j int) bool {
			ret, err := compFunc(values[i], values[j])
			if err != nil {
				fmt.Printf("error while comparing: %v\n", err)
				return false
			}
			return ret > 0
		})
	} else {
		sort.SliceStable(values, func(i, j int) bool {
			ret, err := compFunc(values[i], values[j])
			if err != nil {
				fmt.Printf("error while comparing: %v\n", err)
				return false
			}
			return ret < 0
		})
	}
}
