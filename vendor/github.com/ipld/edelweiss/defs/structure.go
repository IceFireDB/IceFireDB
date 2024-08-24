package defs

type Structure struct {
	Fields []Field
}

func (Structure) Kind() string {
	return "Structure"
}

type Field struct {
	Name   string // on the wire name of field
	GoName string // if not empty, name of field in Go code
	Type   Def
}

type Fields []Field
