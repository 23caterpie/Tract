package tract

// Input specifies a way for a Tract to get requests.
type Input[T any] interface {
	// Get gets the next request. The bool return value is true if a request was gotten.
	// It's false when there is no requests and never will be any more.
	Get() (T, bool)
}

var (
	_ Input[int64] = InputGenerator[int64]{}
)

// TODO: remove this.
// InputGenerator generates request objects.
// It is the default input of a Tract.
type InputGenerator[T any] struct{}

// Get generates the next request.
func (c InputGenerator[T]) Get() (T, bool) {
	var t T
	return t, true
}
