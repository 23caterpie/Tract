package tract

// NewChannel make a new Channel of requests.
func NewChannel[T Request](c chan T) Channel[T] {
	return Channel[T](c)
}

// Channel is a channel of requests.
type Channel[T Request] chan T

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
