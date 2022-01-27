package tract

import "context"

// Input specifies a way for a Tract to get requests.
type Input[T Request] interface {
	// Get gets the next request. The bool return value is true if a request was gotten.
	// It's false when there is no requests and never will be any more.
	Get() (T, bool)
}

var (
	_ Input[int64]                 = Channel[int64](nil)
	_ Input[RequestWrapper[int64]] = NewRequestWrapperInput(Input[int64](nil))
)

// BaseContext specifies a function that returns
// the base context for incoming requests.
// This is the same concept as http.Server.BaseContext
type BaseContext[T any] func(T) context.Context
// TODO: add base time type, so that tracts can measure the first get wait time and a final tract all round latency.

func NewRequestWrapperInput[T Request](
	base Input[T],
) RequestWrapperInput[T] {
	return RequestWrapperInput[T]{
		base:        base,
		BaseContext: func(T) context.Context { return context.Background() },
	}
}

type RequestWrapperInput[T Request] struct {
	base        Input[T]
	BaseContext BaseContext[T]
}

func (i RequestWrapperInput[T]) Get() (RequestWrapper[T], bool) {
	req, ok := i.base.Get()
	return WrapRequestWithContext(i.BaseContext(req), req), ok
}
