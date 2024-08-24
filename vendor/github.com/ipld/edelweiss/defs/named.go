package defs

type Named struct {
	Name string
	Type Def
}

func (Named) Kind() string {
	return "Named"
}
