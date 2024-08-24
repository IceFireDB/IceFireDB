package defs

type Singleton interface{}

type SingletonBool struct {
	Bool bool
}

func (SingletonBool) Kind() string {
	return "SingletonBool"
}

type SingletonFloat struct {
	Float float64
}

func (SingletonFloat) Kind() string {
	return "SingletonFloat"
}

type SingletonInt struct {
	Int int64
}

func (SingletonInt) Kind() string {
	return "SingletonInt"
}

type SingletonByte struct {
	Byte byte
}

func (SingletonByte) Kind() string {
	return "SingletonByte"
}

type SingletonChar struct {
	Char rune
}

func (SingletonChar) Kind() string {
	return "SingletonChar"
}

type SingletonString struct {
	String string
}

func (SingletonString) Kind() string {
	return "SingletonString"
}
