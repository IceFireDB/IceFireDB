package defs

type String struct{}

func (String) Kind() string {
	return "String"
}

type Bytes struct{}

func (Bytes) Kind() string {
	return "Bytes"
}

type Any struct{}

func (Any) Kind() string {
	return "Any"
}

type Nothing struct{}

func (Nothing) Kind() string {
	return "Nothing"
}
