package tract

// Input specifies a way for a Tract to get requests.
type Input[T any] interface {
	// Get gets the next request. The bool return value is true if a request was gotten.
	// It's false when there is no requests and never will be any more.
	Get() (*Request[T], bool)
}

var (
	_ Input[int64] = InputChannel[int64](nil)
	_ Input[int64] = InputGenerator[int64]{}
)

// InputChannel is a channel of requests.
type InputChannel[T any] <-chan *Request[T]

// Get gets the next request from the channel.
func (c InputChannel[T]) Get() (*Request[T], bool) {
	request, ok := <-c
	return request, ok
}

// InputGenerator generates request objects.
// It is the default input of a Tract.
type InputGenerator[T any] struct{}

// Get generates the next request.
func (c InputGenerator[T]) Get() (*Request[T], bool) {
	var req Request[T]
	return &req, true
}
