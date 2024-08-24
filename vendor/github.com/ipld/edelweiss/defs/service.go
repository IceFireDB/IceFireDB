package defs

// Service is a set of named functions.
type Service struct {
	Methods Methods
}

func (Service) Kind() string {
	return "Service"
}

type Method struct {
	Name string
	Type Fn
}

type Methods []Method

func (m Method) Call() Call {
	return Call{ID: String{}, Fn: m.Type}
}

func (m Method) Return() Return {
	return Return{ID: String{}, Fn: m.Type}
}
