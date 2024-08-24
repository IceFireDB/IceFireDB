package defs

type Map struct {
	Key   Def
	Value Def
}

func (Map) Kind() string {
	return "Map"
}
