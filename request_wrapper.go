package tract

import "context"

func WrapRequestWithContext[T Request](
	ctx context.Context,
	base T,
) RequestWrapper[T] {
	return RequestWrapper[T]{
		base: base,
		meta: newRequestWrapperMeta(ctx),
	}
}

func newRequestWrapper[T Request](
	base T,
	meta requestWrapperMeta,
) RequestWrapper[T] {
	return RequestWrapper[T]{
		base: base,
		meta: meta,
	}
}

// RequestWrapper is a struct containing the actual request the user cares about as
// well as some meta data that is used internally.
type RequestWrapper[T Request] struct {
	base T
	meta requestWrapperMeta
}

func (r RequestWrapper[T]) Clone() RequestWrapper[T] {
	return newRequestWrapper(r.base, r.meta.clone())
}

func newRequestWrapperMeta(ctx context.Context) requestWrapperMeta {
	return requestWrapperMeta{
		ctx: ctx,
	}
}

type requestWrapperMeta struct {
	// ctx is used for metrics/tracing.
	// tracing spans are attached to the context for workers to make child spans off of.
	// If a BaseContext function is being used, then this context will be what it specifies,
	// so deadlines, values, or anything on the context will be passed to workers as well.
	ctx context.Context
	// opencensusData is a collection of data used to generate metrics and traces for tracts.
	opencensusData opencensusData
}

func (m requestWrapperMeta) clone() requestWrapperMeta {
	return requestWrapperMeta{
		ctx:            m.ctx,
		opencensusData: m.opencensusData.clone(),
	}
}
