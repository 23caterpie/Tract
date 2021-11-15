package tract

// Request is the object that is passed along the tract.
type Request[T any] struct {
	Data T
}
