package tract

// WorkerFactory makes potentially many Worker objects that may use resources managed by the factory.
type WorkerFactory interface {
	// MakeWorker makes a worker expected to run in a tract.
	MakeWorker() (Worker, error)
	// Close closes factory resources
	Close()
}

// Worker is an object that performs work potentially using it own resources and/or factory resources.
type Worker interface {
	// Work takes a request, performs an operation, and returns that request and a success flag.
	// If the returned bool is false, that specifies that the returned request should be discarded.
	// The expected pattern is to retrieve any needed arguments from the request using request.Value(...)
	// then apply the results of the work to the same request using context.WithValue(request, ...).
	// When designing workers keep the keys for the request values you will be using in mind.
	Work(Request) (Request, bool)
	// Close closes worker resources
	Close()
}

var (
	_ Worker = MetricsWorker{}
)

// MetricsWorker is a wrapper around a Worker that will automatically generate during latency metrics.
type MetricsWorker struct {
	Worker
	metricsHandler MetricsHandler
}

// Work works using the inner Worker while gathering metrics.
func (w MetricsWorker) Work(r Request) (Request, bool) {
	var (
		request Request
		ok      bool
	)
	if w.metricsHandler != nil && w.metricsHandler.ShouldHandle() {
		before := now()
		request, ok = w.Worker.Work(r)
		after := now()
		w.metricsHandler.HandleMetrics(
			Metric{MetricsKeyDuring, after.Sub(before)},
		)
	} else {
		request, ok = w.Worker.Work(r)
	}
	return request, ok
}
