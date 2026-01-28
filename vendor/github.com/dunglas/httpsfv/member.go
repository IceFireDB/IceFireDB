package httpsfv

// Member is a marker interface for members of dictionaries and lists.
//
// See https://httpwg.org/specs/rfc9651.html#list.
type Member interface {
	member()
	marshaler
}
