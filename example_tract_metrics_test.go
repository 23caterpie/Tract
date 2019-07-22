package tract_test

import (
	"fmt"
	"time"

	"git.dev.kochava.com/ccurrin/tract"
)

var _ tract.MetricsHandler = &exampleMetricsHandler{}

func NewExampleMetricsHandler(frequency time.Duration) tract.MetricsHandler {
	return &exampleMetricsHandler{
		DefaultMetricsThrottler: tract.NewDefaultMetricsThrottler(frequency),
	}
}

type exampleMetricsHandler struct {
	tract.DefaultMetricsThrottler
}

func (h *exampleMetricsHandler) HandleMetrics(metrics ...tract.Metric) {
	// Handle the metrics
	// Send them to influx or something.
	for _, metric := range metrics {
		fmt.Printf("%+v\n", metric)
	}
}

// ExampleMetricsHandler shows an example of using a metrics handler in a Tract.
// This provides a way for the user to gather metrics around each worker tract.
func ExampleMetricsHandler() {
	myMetricsHandler := NewExampleMetricsHandler(10 * time.Second)

	myTract := tract.NewSerialGroupTract("my tract",
		// ...
		tract.NewWorkerTract("square root", 4, tract.NewFactoryFromWorker(SquareRootWorker{}), tract.WithFactoryClosure(true), tract.WithMetricsHandler(myMetricsHandler)),
		// ...
	)

	err := myTract.Init()
	if err != nil {
		// Handle error
		return
	}

	myTract.Start()()
}
