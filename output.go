package tract

// Output specifies a way for a Tract pass requests along.
type Output[T any] interface {
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
	_ Output[int64] = Outputs[int64, Output[int64]](nil)
	_ Output[int64] = FinalOutput[int64]{}
	_ Output[int64] = nonCloseOutput[int64]{}
)

type Outputs[T any, D Output[T]] []D

// Put puts on all outputs.
func (os Outputs[T, D]) Put(t T) {
	for _, o := range os {
		o.Put(t)
	}
}

// Close closes all the outputs.
func (os Outputs[T, D]) Close() {
	for _, o := range os {
		o.Close()
	}
}

// FinalOutput is the last output for requests.
// Requests that are outputted here have reached the end of their life.
// It is the default output of a Tract.
type FinalOutput[T any] struct{}

// Put sinks the request (noop).
func (c FinalOutput[T]) Put(T) {}

// Close is a noop.
func (c FinalOutput[_]) Close() {}

// nonCloseOutput is an Output wrapper that turns the `Close()` method to a noop.
// Used in group tracts that can possibly fan request into an output, thus requiring
// the group tract to handling closing for all inner tracts.
type nonCloseOutput[T any] struct {
	Output[T]
}

func (c nonCloseOutput[_]) Close() {}
