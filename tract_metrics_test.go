package tract

import (
	"context"
	"testing"
	"time"
)

type testMetricHandler struct {
	metricsChannel chan<- Metric
}

func (h testMetricHandler) HandleMetrics(metrics ...Metric) {
	for _, metric := range metrics {
		h.metricsChannel <- metric
	}
}

func (h testMetricHandler) ShouldHandle() bool { return true }

type testWorker struct {
	newTime time.Time
	work    func(r Request) (Request, bool)
}

func (w testWorker) Work(r Request) (Request, bool) {
	return w.work(r)
}

func (w testWorker) Close() {}

type testInput struct {
	preGetFunc func()
	Input
}

func (i *testInput) Get() (Request, bool) {
	i.preGetFunc()
	return i.Input.Get()
}

type testOutput struct {
	prePutFunc func()
	Output
}

func (o *testOutput) Put(r Request) {
	o.prePutFunc()
	o.Output.Put(r)
}

// TestWithMetricsHandler is a rigid test that tests the metric handler in a worker tract.
// If this test breaks due to an implementation change, it can probably be dropped.
func TestWithMetricsHandler(t *testing.T) {
	workerWaiterChannel := make(chan struct{})
	workerNewTime := new(time.Time)
	metricsChannel := make(chan Metric)
	workerTract := NewWorkerTract("waiter", 1,
		NewFactoryFromWorker(testWorker{
			work: func(r Request) (Request, bool) {
				<-workerWaiterChannel
				now = func() time.Time { return *workerNewTime }
				return r, true
			},
		}),
		WithFactoryClosure(true),
		WithMetricsHandler(testMetricHandler{
			metricsChannel: metricsChannel,
		}),
	)

	inputChannel := make(chan Request)
	input := &testInput{
		preGetFunc: func() {},
		Input:      InputChannel(inputChannel),
	}
	workerTract.SetInput(input)
	outputChannel := make(chan Request)
	output := &testOutput{
		prePutFunc: func() {},
		Output:     OutputChannel(outputChannel),
	}
	workerTract.SetOutput(output)

	err := workerTract.Init()
	if err != nil {
		t.Fatalf("unexpected error during tract initialization %v", err)
	}

	now = func() time.Time { return time.Date(2019, time.July, 22, 0, 0, 0, 0, time.UTC) }
	input.preGetFunc = func() { now = func() time.Time { return time.Date(2019, time.July, 22, 0, 0, 1, 0, time.UTC) } } // 1 second duration

	waitOnTract := workerTract.Start()

	inputChannel <- context.Background()
	metric := <-metricsChannel
	expectedMetric := Metric{Key: MetricsKeyIn, Value: 1 * time.Second}
	if metric != expectedMetric {
		t.Errorf("unexpected metric for tract input expected: %+#v, recieved: %+#v", expectedMetric, metric)
	}

	*workerNewTime = time.Date(2019, time.July, 22, 0, 1, 0, 0, time.UTC) // 59 second duration
	workerWaiterChannel <- struct{}{}
	metric = <-metricsChannel
	expectedMetric = Metric{Key: MetricsKeyDuring, Value: 59 * time.Second}
	if metric != expectedMetric {
		t.Errorf("unexpected metric for tract input expected: %+#v, recieved: %+#v", expectedMetric, metric)
	}

	output.prePutFunc = func() { now = func() time.Time { return time.Date(2019, time.July, 22, 1, 0, 0, 0, time.UTC) } } // 59 minute duration
	<-outputChannel
	metric = <-metricsChannel
	expectedMetric = Metric{Key: MetricsKeyOut, Value: 59 * time.Minute}
	if metric != expectedMetric {
		t.Errorf("unexpected metric for tract input expected: %+#v, recieved: %+#v", expectedMetric, metric)
	}

	input.preGetFunc = func() { now = func() time.Time { return time.Date(2019, time.July, 23, 0, 0, 0, 0, time.UTC) } } // 23 hour duration
	close(inputChannel)
	metric = <-metricsChannel
	expectedMetric = Metric{Key: MetricsKeyIn, Value: 23 * time.Hour}
	if metric != expectedMetric {
		t.Errorf("unexpected metric for tract input expected: %+#v, recieved: %+#v", expectedMetric, metric)
	}
	waitOnTract()
}
