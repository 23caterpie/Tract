package tract

import "time"

// test overridable time.Now function used for metrics gathering
var now = time.Now

// Metric is a tuple of a metric time latency and a key specifying what the metric is measuring.
type Metric struct {
	Key   MetricsKey
	Value time.Duration
}

// MetricsKey is an enum key that specifies a kind of metric
type MetricsKey int

const (
	// MetricsKeyIn specifiies metric for the amount of time a tract spent waiting for the next request from its input.
	MetricsKeyIn MetricsKey = iota + 1
	// MetricsKeyDuring specifiies metric for the amount of time a tract spent waiting for its worker to process a request.
	MetricsKeyDuring
	// MetricsKeyOut specifiies metric for the amount of time a tract spent waiting to output a request to its output.
	MetricsKeyOut
	// MetricsKeyTract specifiies metric for the amount of time from when a request was generated,
	// until it hit the end of the tract (was outputted from a tract that had no user specified output).
	MetricsKeyTract
)

// MetricsHandler handles metrics that a tract produces.
type MetricsHandler interface {
	// HandleMetrics is the method where all metrics are passed to for handling. It should compare the
	// Key of each metric against the list of metric key constants to determine what each metric means.
	HandleMetrics(...Metric)
	// ShouldHandle should return true if the MetricsHandler is ready for more metrics, otherwise false.
	// This is used by the tract to determine if metrics should even be generated. This function should
	// return as fast as possible as it will be called for every request within every tract this
	// MetricsHandler is used.
	ShouldHandle() bool
}

// NewDefaultMetricsThrottler makes a DefaultMetricsThrottle ready to use
func NewDefaultMetricsThrottler(frequency time.Duration) DefaultMetricsThrottler {
	throttler := DefaultMetricsThrottler{
		frequency: frequency,
	}
	if frequency > 0 {
		throttler.timer = time.NewTimer(frequency)
	}
	return throttler
}

// DefaultMetricsThrottler is a provided implementation of ShouldHandle()
// that can be composed into any struct trying to implement a MetricsHandler.
type DefaultMetricsThrottler struct {
	frequency time.Duration
	timer     *time.Timer
}

// ShouldHandle determines when we should handle metrics based off a frequency
//  Frequency |            ShouldHandle() logic
//  --------------------------------------------
//          0 |                   Always handle
//        < 0 |                    Never handle
//        > 0 | Handle once per frequency cycle
func (d DefaultMetricsThrottler) ShouldHandle() bool {
	// Default/Edge case when frequency is zero, we should always handle
	if d.frequency == 0 {
		return true
	}
	// Timer is nil if frequency is negative (never should handle)
	if d.timer == nil {
		return false
	}
	shouldHandle := false
	select {
	case <-d.timer.C:
		shouldHandle = true
		d.timer.Reset(d.frequency)
	default:
	}
	return shouldHandle
}

var (
	_ MetricsHandler = composeDefaultMetricsThrottlerMetricsHandler{}
	_ MetricsHandler = &composeDefaultMetricsThrottlerMetricsHandler{}
	_ MetricsHandler = &manualOverrideMetricsHandler{}
)

type manualOverrideMetricsHandler struct {
	MetricsHandler
	shouldHandle bool
}

func (h *manualOverrideMetricsHandler) ShouldHandle() bool {
	return h.shouldHandle
}

func (h *manualOverrideMetricsHandler) SetShouldHandle(should bool) {
	h.shouldHandle = should
}

// composeDefaultMetricsThrottlerMetricsHandler exists as a compilation test and example for using a DefaultMetricsThrottler composed in a struct
type composeDefaultMetricsThrottlerMetricsHandler struct {
	DefaultMetricsThrottler
}

func (composeDefaultMetricsThrottlerMetricsHandler) HandleMetrics(...Metric) {}
