package tract

// Input specifies a way for a Tract to get requests.
type Input[T any] interface {
	// Get gets the next request. The bool return value is true if a request was gotten.
	// It's false when there is no requests and never will be any more.
	Get() (Request[T], bool)
}

var (
	_ Input[int64] = InputChannel[int64](nil)
	_ Input[int64] = InputGenerator[int64]{}
	_ Input[int64] = MetricsInput[int64]{}
)

// InputChannel is a channel of requests.
type InputChannel[T any] <-chan Request[T]

// Get gets the next request from the channel.
func (c InputChannel[T]) Get() (Request[T], bool) {
	request, ok := <-c
	return request, ok
}

// InputGenerator generates request objects.
// It is the default input of a Tract.
type InputGenerator[T any] struct{}

// Get generates the next request.
// The current time is stored in the request at this generation time.
// It can be retrieved by using GetRequestStartTime().
func (c InputGenerator[T]) Get() (Request[T], bool) {
	var req Request[T]
	setRequestStartTime(&req, now())
	return req, true
}

// MetricsInput is a wrapper around an Input that will automatically generate input latency metrics
type MetricsInput[T any] struct {
	Input[T]
	metricsHandler MetricsHandler
}

// Get gets from the inner input while gathering metrics.
func (i MetricsInput[T]) Get() (Request[T], bool) {
	var (
		request Request[T]
		ok      bool
	)
	if i.metricsHandler != nil && i.metricsHandler.ShouldHandle() {
		before := now()
		request, ok = i.Input.Get()
		after := now()
		i.metricsHandler.HandleMetrics(
			Metric{MetricsKeyIn, after.Sub(before)},
		)
	} else {
		request, ok = i.Input.Get()
	}
	return request, ok
}
