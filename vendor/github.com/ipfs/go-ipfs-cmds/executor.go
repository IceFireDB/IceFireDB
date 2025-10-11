package cmds

import (
	"context"
)

type Executor interface {
	Execute(req *Request, re ResponseEmitter, env Environment) error
}

// Environment is the environment passed to commands.
type Environment interface{}

// MakeEnvironment takes a context and the request to construct the environment
// that is passed to the command's Run function.
// The user can define a function like this to pass it to cli.Run.
type MakeEnvironment func(context.Context, *Request) (Environment, error)

// MakeExecutor takes the request and environment variable to construct the
// executor that determines how to call the command - i.e. by calling Run or
// making an API request to a daemon.
// The user can define a function like this to pass it to cli.Run.
type MakeExecutor func(*Request, interface{}) (Executor, error)

func NewExecutor(root *Command) Executor {
	return &executor{
		root: root,
	}
}

type executor struct {
	root *Command
}

func (x *executor) Execute(req *Request, re ResponseEmitter, env Environment) error {
	cmd := req.Command

	if cmd.Run == nil {
		return ErrNotCallable
	}

	err := cmd.CheckArguments(req)
	if err != nil {
		return err
	}

	if cmd.PreRun != nil {
		err = cmd.PreRun(req, env)
		if err != nil {
			return err
		}
	}
	maybeStartPostRun := func(formatters PostRunMap) <-chan error {
		var (
			postRun   func(Response, ResponseEmitter) error
			postRunCh = make(chan error)
		)

		// Check if we have a formatter for this emitter type.
		typer, isTyper := re.(interface {
			Type() PostRunType
		})
		if isTyper {
			postRun = formatters[typer.Type()]
		}
		// If not, just return nil via closing.
		if postRun == nil {
			close(postRunCh)
			return postRunCh
		}

		// Otherwise, relay emitter responses
		// from Run to PostRun, and
		// from PostRun to the original emitter.
		var (
			postRes     Response
			postEmitter = re
		)
		re, postRes = NewChanResponsePair(req)
		go func() {
			defer close(postRunCh)
			postRunCh <- postEmitter.CloseWithError(postRun(postRes, postEmitter))
		}()
		return postRunCh
	}

	postRunCh := maybeStartPostRun(cmd.PostRun)
	runCloseErr := re.CloseWithError(cmd.Run(req, re, env))
	postCloseErr := <-postRunCh
	switch runCloseErr {
	case ErrClosingClosedEmitter, nil:
	default:
		return runCloseErr
	}
	switch postCloseErr {
	case ErrClosingClosedEmitter, nil:
	default:
		return postCloseErr
	}
	return nil
}
