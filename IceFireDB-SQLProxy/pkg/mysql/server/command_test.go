package server

// Define the Handler interface
type HandlerTest interface {
	Serve()
	CloseConn(*Conn) error
}

// Define the EmptyHandler struct
type EmptyHandlerTest struct{}

// Implement the Serve method for EmptyHandler
func (e EmptyHandlerTest) Serve() {
	// Implementation of Serve method
}

// Implement the CloseConn method for EmptyHandler
func (e EmptyHandlerTest) CloseConn(c *Conn) error {
	// Implementation of CloseConn method
	// For example, close the connection
	return nil
}

// Ensure EmptyHandler implements HandlerTest interface or cause compile time error
var _ HandlerTest = EmptyHandlerTest{}
