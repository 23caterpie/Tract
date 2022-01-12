package tract

import "context"

type Request interface {
	any
}

// ContextRequest is a way to get and attach contexts to the request.
// Requests can optionally satisfy this interface for use with middleware.
type ContextRequest interface {
	Request
	// Context returns the context off the request.
	Context() context.Context
}
