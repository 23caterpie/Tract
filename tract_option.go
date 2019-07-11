package tract

// WorkerTractOption is a function option applyable to worker tracts.
type WorkerTractOption func(*workerTract)

// WithMetricsHandler creates a WorkerTractOption that will set the tract's metrics handler to the provided one.
func WithMetricsHandler(mh MetricsHandler) WorkerTractOption {
	return func(p *workerTract) {
		p.metricsHandler = mh
	}
}
