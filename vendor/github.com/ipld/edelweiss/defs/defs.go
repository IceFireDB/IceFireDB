package defs

type Def interface {
	Kind() string
}

type Defs []Def
