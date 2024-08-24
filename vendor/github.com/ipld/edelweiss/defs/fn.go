package defs

// Fn represents the type signature of a function.
type Fn struct {
	Arg    Def
	Return Def
}

func (Fn) Kind() string {
	return "Fn"
}

// Call is the type representing a function call (aka request)
type Call struct {
	Fn Fn  // type signature of the function being called
	ID Def // function instance identifier (can be a user-defined type)
}

func (Call) Kind() string {
	return "Call"
}

// Return is the type representing a function result (aka response)
type Return struct {
	Fn Fn  // type signature of the function returning a result
	ID Def // function instance identifier (can be a user-defined type)
}

func (Return) Kind() string {
	return "Return"
}
