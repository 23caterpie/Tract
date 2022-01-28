package tract

import "context"
import "time"
import "go.opencensus.io/stats"
import "go.opencensus.io/tag"

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

func (r RequestWrapper[T]) Clone(times int32) []RequestWrapper[T] {
	ocDatas := r.meta.opencensusData.clone(times)
	clones := make([]RequestWrapper[T], len(ocDatas))
	for i := range ocDatas {
		req := r
		req.meta.opencensusData = ocDatas[i]
		clones[i] = req
	}
	return clones
}

func newRequestWrapperMeta(ctx context.Context) requestWrapperMeta {
	return requestWrapperMeta{
		opencensusData: newOpencensusData(ctx),
	}
}

type requestWrapperMeta struct {
	// opencensusData is a collection of data used to generate metrics and traces for tracts.
	opencensusData opencensusData
}

// Input/Output

func NewRequestWrapperLinks[T, D Request](
	baseInput Input[T],
	baseOutput Output[D],
) (
	RequestWrapperInput[T],
	RequestWrapperOutput[D],
) {
	return newRequestWrapperInput(baseInput),
		newRequestWrapperOutput(baseOutput)
}

// BaseContext specifies a function that returns
// the base context for incoming requests.
// This is the same concept as http.Server.BaseContext
type BaseContext[T any] func(T) context.Context

func newRequestWrapperInput[T Request](
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

func newRequestWrapperOutput[T Request](base Output[T]) RequestWrapperOutput[T] {
	return RequestWrapperOutput[T]{
		base: base,
	}
}

type RequestWrapperOutput[T Request] struct {
	base Output[T]
}

func (o RequestWrapperOutput[T]) Put(r RequestWrapper[T]) {
	o.base.Put(r.base)
	end := now()
	// Pop the all data as to leave no dangling spans.
	for !r.meta.opencensusData.popInputData().IsZero() {}
	_ = r.meta.opencensusData.popAllOutputData()
	// Use base to creat base metrics and traces.
	base := r.meta.opencensusData.baseData
	base.endSpan()
	stats.RecordWithTags(base.ctx,
		[]tag.Mutator{
			tag.Upsert(GroupName, "octract/base"),
		},
		GroupWorkLatency.M(float64(end.Sub(base.timestamp))/float64(time.Millisecond)),
	)
}

func (o RequestWrapperOutput[T]) Close() {
	o.base.Close()
}
