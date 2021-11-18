package tract

type link[T any] interface {
	Input[T]
	Output[T]
}

var (
	_ link[int64] = Channel[int64](nil)
)

// Channel is a channel of requests.
type Channel[T any] chan T

// Put puts the request onto the channel.
func (c Channel[T]) Put(t T) {
	c <- t
}

// Close closes the channel.
func (c Channel[T]) Close() {
	close(c)
}

// Get gets the next request from the channel.
func (c Channel[T]) Get() (T, bool) {
	request, ok := <-c
	return request, ok
}
