package defs

// Bool, Float, Int, Byte, Char

type Bool struct{}

func (Bool) Kind() string {
	return "Bool"
}

type Float struct{}

func (Float) Kind() string {
	return "Float"
}

type Int struct{}

func (Int) Kind() string {
	return "Int"
}

type Byte struct{}

func (Byte) Kind() string {
	return "Byte"
}

type Char struct{}

func (Char) Kind() string {
	return "Char"
}
