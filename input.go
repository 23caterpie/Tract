package tract

import (
	"context"
)

// Input specifies a way for a Tract to get requests.
type Input interface {
	// Get gets the next request. The bool return value is true if a request was gotten.
	// It's false when there is no requests and never will be any more.
	Get() (Request, bool)
}

var (
	_ Input = InputChannel(nil)
	_ Input = InputGenerator{}
	_ Input = MetricsInput{}
)

// InputChannel is a channel of requests.
type InputChannel <-chan Request

// Get gets the next request from the channel.
func (c InputChannel) Get() (Request, bool) {
	request, ok := <-c
	return request, ok
}

// InputGenerator generates request objects.
// It is the default input of a Tract.
type InputGenerator struct{}

// Get generates the next request.
// The current time is stored in the request at this generation time.
// It can be retrieved by using GetRequestStartTime().
func (c InputGenerator) Get() (Request, bool) {
	return setRequestStartTime(context.Background(), now()), true
}

// MetricsInput is a wrapper around an Input that will automatically generate input latency metrics
type MetricsInput struct {
	Input
	metricsHandler MetricsHandler
}

// Get gets from the inner input while gathering metrics.
func (i MetricsInput) Get() (Request, bool) {
	var (
		request Request
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
