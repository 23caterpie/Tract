package tract

// WorkerTractOption is a function option applyable to worker tracts.
type WorkerTractOption[T any] func(*workerTract[T])

// WithMetricsHandler creates a WorkerTractOption that will set the tract's metrics handler to the provided one.
// By default no metrics handler is used, and thus no metrics are gathered.
func WithMetricsHandler[T any](mh MetricsHandler) WorkerTractOption[T] {
	return func(p *workerTract[T]) {
		p.metricsHandler = mh
	}
}

// WithFactoryClosure creates a WorkerTractOption that will specify if the Tract should close
// its WorkerFactory when the tract is finished running when its Start closure is called.
// By default factories are not closed when a Tract is closed.
// This allows a Tract to be restarted, but forces the user to close their own factories.
// If specified that the factory should close, then the tract cannot safely be restarted,
// but the user won't have to manually close their factory.
func WithFactoryClosure[T any](shouldClose bool) WorkerTractOption[T] {
	return func(p *workerTract[T]) {
		p.shouldCloseFactory = shouldClose
	}
}
