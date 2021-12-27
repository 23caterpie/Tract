package tract

import "context"

// import "constraints"

// type Request interface {
// 	constraints.Ordered
// }

type Request interface {
	any
}

// ContextRequest is a way to get and attach contexts to the request.
// Requests can optionally satisfy this interface for use with middleware.
type ContextRequest interface {
	Request
	// Context returns the context off the request and a thread safe set function to set the context back on the request.
	// setCtx is expected to be called in order to unblock future Context() calls.
	// TODO: maybe change this back since context's are inherently thread safe...
	Context() (ctx context.Context, setCtx func(context.Context))
}
