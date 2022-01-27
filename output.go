package tract

// Output specifies a way for a Tract pass requests along.
type Output[T Request] interface {
	// Put outputs the the request.
	// Should never be called once Close has been called.
	Put(T)
	// Close closes the output. No more requests should be outputted.
	// Put should not be called once Close has been called.
	// If there is something on the other side of this output receiving
	// requests, it should be notified that there are no more requests.
	Close()
}

var (
	_ Output[int64]                 = Channel[int64](nil)
	_ Output[RequestWrapper[int64]] = outputs[int64, Output[RequestWrapper[int64]]](nil)
	_ Output[int64]                 = nonCloseOutput[int64]{}
	_ Output[RequestWrapper[int64]] = NewRequestWrapperOutput(Output[int64](nil))
)

type outputs[T Request, D Output[RequestWrapper[T]]] []D

// Put puts on all outputs.
func (os outputs[T, D]) Put(req RequestWrapper[T]) {
	clones := req.Clone(len(os))
	for i, o := range os {
		o.Put(clones[i])
	}
}

// Close closes all the outputs.
func (os outputs[T, D]) Close() {
	for _, o := range os {
		o.Close()
	}
}

// nonCloseOutput is an Output wrapper that turns the `Close()` method to a noop.
// Used in group tracts that can possibly fan request into an output, thus requiring
// the group tract to handling closing for all inner tracts.
type nonCloseOutput[T Request] struct {
	Output[T]
}

func (c nonCloseOutput[_]) Close() {}

func NewRequestWrapperOutput[T Request](base Output[T]) RequestWrapperOutput[T] {
	return RequestWrapperOutput[T]{
		base: base,
	}
}

type RequestWrapperOutput[T Request] struct {
	base Output[T]
}

func (o RequestWrapperOutput[T]) Put(r RequestWrapper[T]) {
	// Pop the output data as to leave no dangling spans.
	_ = r.meta.opencensusData.popAllOutputData()
	// TODO: Add metrics here?
	o.base.Put(r.base)
}

func (o RequestWrapperOutput[T]) Close() {
	o.base.Close()
}
