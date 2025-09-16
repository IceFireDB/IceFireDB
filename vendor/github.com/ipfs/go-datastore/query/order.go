package query

import (
	"bytes"
	"slices"
	"strings"
)

// Order is an object used to order objects
type Order interface {
	Compare(a, b Entry) int
}

// OrderByFunction orders the results based on the result of the given function.
type OrderByFunction func(a, b Entry) int

func (o OrderByFunction) Compare(a, b Entry) int {
	return o(a, b)
}

func (OrderByFunction) String() string {
	return "FN"
}

// OrderByValue is used to signal to datastores they should apply internal
// orderings.
type OrderByValue struct{}

func (o OrderByValue) Compare(a, b Entry) int {
	return bytes.Compare(a.Value, b.Value)
}

func (OrderByValue) String() string {
	return "VALUE"
}

// OrderByValueDescending is used to signal to datastores they
// should apply internal orderings.
type OrderByValueDescending struct{}

func (o OrderByValueDescending) Compare(a, b Entry) int {
	return -bytes.Compare(a.Value, b.Value)
}

func (OrderByValueDescending) String() string {
	return "desc(VALUE)"
}

// OrderByKey
type OrderByKey struct{}

func (o OrderByKey) Compare(a, b Entry) int {
	return strings.Compare(a.Key, b.Key)
}

func (OrderByKey) String() string {
	return "KEY"
}

// OrderByKeyDescending
type OrderByKeyDescending struct{}

func (o OrderByKeyDescending) Compare(a, b Entry) int {
	return -strings.Compare(a.Key, b.Key)
}

func (OrderByKeyDescending) String() string {
	return "desc(KEY)"
}

// Less returns true if a comes before b with the requested orderings.
func Less(orders []Order, a, b Entry) bool {
	for _, order := range orders {
		switch order.Compare(a, b) {
		case 0:
		case -1:
			return true
		case 1:
			return false
		}
	}

	// This gives us a *stable* sort for free. We don't care
	// preserving the order from the underlying datastore
	// because it's undefined.
	return a.Key < b.Key
}

// Compare compares two Entry values according to the given orders. Returns -1
// if Entry a comes before b with the requested ordering, 1 if a comes after b,
// and 0 if a and b are the same.
func Compare(orders []Order, a, b Entry) int {
	for _, order := range orders {
		n := order.Compare(a, b)
		if n != 0 {
			return n
		}
	}
	// Gives stable sort.
	return strings.Compare(a.Key, b.Key)
}

// Sort sorts the given entries using the given orders.
func Sort(orders []Order, entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		return Compare(orders, a, b)
	})
}
