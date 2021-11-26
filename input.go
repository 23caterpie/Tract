package tract

// Input specifies a way for a Tract to get requests.
type Input[T any] interface {
	// Get gets the next request. The bool return value is true if a request was gotten.
	// It's false when there is no requests and never will be any more.
	Get() (T, bool)
}
