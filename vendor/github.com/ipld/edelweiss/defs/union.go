package defs

type Union struct {
	Cases Cases
}

func (Union) Kind() string {
	return "Union"
}

type Case struct {
	Name   string // name on the wire
	GoName string // if not empty, name in Go code
	Type   Def
}

type Cases []Case
