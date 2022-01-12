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

// // TEST SHIZ

// // ContextRequest is a way to get and attach contexts to the request.
// // Requests can optionally satisfy this interface for use with middleware.
// type ContextRequest[T any] interface {
// 	Request
// 	// Context returns the context off the request.
// 	Context() context.Context
// 	// WithContext returns a copy of the ContextRequest with the context replaced.
// 	// To make your request safe for use in concurrent fanout tracts,
// 	// this should return an actual copy and not return the same pointer.
// 	WithContext(context.Context) T
// }

// type Cloner[T any] interface {
// 	WithContext(ctx context.Context) T
// }

// func NewClonee[T Cloner[T]](t T) Clonee[T] {
// 	return Clonee[T]{
// 		base: t,
// 	}
// }

// type Clonee[T Cloner[T]] struct {
// 	base T
// }

// func (c *Clonee[T]) copy(ctx context.Context) {
// 	c.base = c.base.WithContext(ctx)
// }

// var (
// 	_ = NewClonee(XClonee{ctx: context.Background()})
// 	_ Cloner[XClonee] = XClonee{ctx: context.Background()}
// )

// type XClonee struct {
// 	ctx context.Context
// }

// func (x XClonee) WithContext(ctx context.Context) XClonee {
// 	x.ctx = ctx
// 	return x
// }
