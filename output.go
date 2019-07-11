package tract

// Output specifies a way for a Tract pass requests along.
type Output interface {
	// Put outputs the the request.
	// Should never be called once Close has been called.
	Put(Request)
	// Close closes the output. No more requests should be outputted.
	// Put should not be called once Close has been called.
	// If there is something on the other side of this output receiving
	// requests, it should be notified that there are no more requests.
	Close()
}

var (
	_ Output = OutputChannel(nil)
	_ Output = FinalOutput{}
	_ Output = MetricsOutput{}
)

// OutputChannel is a channel of requests.
type OutputChannel chan<- Request

// Put puts the request onto the channel.
func (c OutputChannel) Put(r Request) {
	c <- r
}

// Close closes the channel.
func (c OutputChannel) Close() {
	close(c)
}

// FinalOutput is the last output for requests.
// Requests that are outputted here have reached the end of their life.
// It is the default output of a Tract.
type FinalOutput struct{}

// Put sinks the request (noop).
func (c FinalOutput) Put(r Request) {}

// Close is a noop.
func (c FinalOutput) Close() {}

// MetricsOutput is a wrapper around an Output that will automatically generate output latency metrics
type MetricsOutput struct {
	Output
	metricsHandler MetricsHandler
}

// Put outputs to the inner output while gathering metrics.
func (o MetricsOutput) Put(r Request) {
	if o.metricsHandler != nil && o.metricsHandler.ShouldHandle() {
		before := now()
		o.Output.Put(r)
		after := now()
		if _, ok := o.Output.(FinalOutput); ok {
			o.metricsHandler.HandleMetrics(
				Metric{MetricsKeyOut, after.Sub(before)},
				Metric{MetricsKeyTract, after.Sub(GetRequestStartTime(r))},
			)
		} else {
			o.metricsHandler.HandleMetrics(
				Metric{MetricsKeyOut, after.Sub(before)},
			)
		}
	} else {
		o.Output.Put(r)
	}
}
