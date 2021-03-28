package tract_test

import (
	"fmt"
	"time"

	tract "github.com/23caterpie/Tract"
)

var _ tract.MetricsHandler = &exampleMetricsHandler{}

func NewExampleMetricsHandler(name string, frequency time.Duration) tract.MetricsHandler {
	return &exampleMetricsHandler{
		sourceTractName:         name,
		DefaultMetricsThrottler: tract.NewDefaultMetricsThrottler(frequency),
	}
}

type exampleMetricsHandler struct {
	sourceTractName string
	tract.DefaultMetricsThrottler
}

func (h *exampleMetricsHandler) HandleMetrics(metrics ...tract.Metric) {
	// Handle the metrics
	// Send them to influx or something.
	for _, metric := range metrics {
		// check the metrics key to determine what this metric is for.
		// You don't have to handle every key type.
		var metricsKey string
		switch metric.Key {
		// How long did we spend waiting for the next request?
		case tract.MetricsKeyIn:
			metricsKey = "in"
		// How long did we spend working on the request?
		case tract.MetricsKeyDuring:
			metricsKey = "during"
		// How long did we spend waiting to output the request?
		case tract.MetricsKeyOut:
			metricsKey = "out"
		// The request has reached the end of the line. How long since the request was created?
		case tract.MetricsKeyTract:
			metricsKey = "tract"
		// Either an invalid metrics key, one we don't know about, or one we don't care about.
		default:
			metricsKey = "unknown"
		}
		fmt.Printf("%s :: %s :: %v\n", h.sourceTractName, metricsKey, metric.Value)
	}
}

// ExampleMetricsHandler shows an example of using a metrics handler in a Tract.
// This provides a way for the user to gather metrics around each worker tract.
func ExampleMetricsHandler() {
	// Each worker tract using a metrics handler should use their own metrics handler
	// if using the tract.DefaultMetricsThrottler, so they each throttle separately.
	// In this case, our handler is given context of what Tract it's handling in its
	// contructor, so separate handlers for each Tract is needed for that as well.
	myTract := tract.NewSerialGroupTract("my tract",
		// ...
		tract.NewWorkerTract("square root", 4,
			tract.NewFactoryFromWorker(SquareRootWorker{}),
			tract.WithMetricsHandler(NewExampleMetricsHandler("squareroot", 10*time.Second)),
		),
		tract.NewWorkerTract("some other worker", 2,
			tract.NewFactoryFromWorker(SquareRootWorker{}),
			tract.WithMetricsHandler(NewExampleMetricsHandler("somethingelse", 10*time.Second)),
		),
		// ...
	)

	err := myTract.Init()
	if err != nil {
		// Handle error
		return
	}

	myTract.Start()()
}
