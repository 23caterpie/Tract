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

func (r RequestWrapper[T]) Clone(times int) []RequestWrapper[T] {
	clones := make([]RequestWrapper[T], times)
	for i := range clones {
		if i == 0 {
			r.meta.opencensusData.block(times)
			clones[i] = r
		} else {
			clones[i] = newRequestWrapper(r.base, r.meta.clone(times))
		}
	}
	return clones
}

func newRequestWrapperMeta(ctx context.Context) requestWrapperMeta {
	return requestWrapperMeta{
		opencensusData: opencensusData{
			ctx: ctx,
		},
	}
}

type requestWrapperMeta struct {
	// opencensusData is a collection of data used to generate metrics and traces for tracts.
	opencensusData opencensusData
}

func (m requestWrapperMeta) clone(amount int) requestWrapperMeta {
	return requestWrapperMeta{
		opencensusData: m.opencensusData.clone(amount),
	}
}
