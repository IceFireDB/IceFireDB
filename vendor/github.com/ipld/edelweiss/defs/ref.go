package defs

// Ref is a reference to a type by name.
type Ref struct {
	Name string
}

type Refs []Ref

func (Ref) Kind() string {
	return "Ref"
}
