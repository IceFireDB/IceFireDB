package defs

type List struct {
	Element Def
}

func (List) Kind() string {
	return "List"
}
