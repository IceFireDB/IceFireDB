package defs

type Tuple struct {
	Slots Slots
}

type Slots Defs

func (Tuple) Kind() string {
	return "Tuple"
}
