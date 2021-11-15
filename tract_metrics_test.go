package tract

import (
	"sync"
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

type testWorker[T any] struct {
	newTime time.Time
	work    func(r Request[T]) (Request[T], bool)
}

func (w testWorker[T]) Work(r Request[T]) (Request[T], bool) {
	return w.work(r)
}

func (w testWorker[_]) Close() {}

type testInput[T any] struct {
	preGetFunc      func()
	preGetFuncMutex sync.Mutex
	Input[T]
}

func (i *testInput[T]) Get() (Request[T], bool) {
	i.preGetFuncMutex.Lock()
	i.preGetFunc()
	i.preGetFuncMutex.Unlock()
	return i.Input.Get()
}

func (i *testInput[_]) setPreGetFunc(f func()) {
	i.preGetFuncMutex.Lock()
	i.preGetFunc = f
	i.preGetFuncMutex.Unlock()
}

type testOutput[T any] struct {
	prePutFunc      func()
	prePutFuncMutex sync.Mutex
	Output[T]
}

func (o *testOutput[T]) Put(r Request[T]) {
	o.prePutFuncMutex.Lock()
	o.prePutFunc()
	o.prePutFuncMutex.Unlock()
	o.Output.Put(r)
}

func (o *testOutput[_]) setPrePutFunc(f func()) {
	o.prePutFuncMutex.Lock()
	o.prePutFunc = f
	o.prePutFuncMutex.Unlock()
}

// TestWithMetricsHandler is a rigid test that tests the metric handler in a worker tract.
// If this test breaks due to an implementation change, it can probably be dropped.
func TestWithMetricsHandler(t *testing.T) {
	workerWaiterChannel := make(chan struct{})
	workerNewTime := new(time.Time)
	metricsChannel := make(chan Metric)
	type myRequestType struct{}
	workerTract := NewWorkerTract("waiter", 1,
		NewFactoryFromWorker[myRequestType](testWorker[myRequestType]{
			work: func(r Request[myRequestType]) (Request[myRequestType], bool) {
				<-workerWaiterChannel
				now = func() time.Time { return *workerNewTime }
				return r, true
			},
		}),
		WithFactoryClosure[myRequestType](true),
		WithMetricsHandler[myRequestType](testMetricHandler{
			metricsChannel: metricsChannel,
		}),
	)

	inputChannel := make(chan Request[myRequestType])
	input := &testInput[myRequestType]{
		preGetFunc: func() {},
		Input:      InputChannel[myRequestType](inputChannel),
	}
	workerTract.SetInput(input)
	outputChannel := make(chan Request[myRequestType])
	output := &testOutput[myRequestType]{
		prePutFunc: func() {},
		Output:     OutputChannel[myRequestType](outputChannel),
	}
	workerTract.SetOutput(output)

	err := workerTract.Init()
	if err != nil {
		t.Fatalf("unexpected error during tract initialization %v", err)
	}

	now = func() time.Time { return time.Date(2019, time.July, 22, 0, 0, 0, 0, time.UTC) }
	input.setPreGetFunc(func() { now = func() time.Time { return time.Date(2019, time.July, 22, 0, 0, 1, 0, time.UTC) } }) // 1 second duration

	waitOnTract := workerTract.Start()

	inputChannel <- Request[myRequestType]{}
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

	output.setPrePutFunc(func() { now = func() time.Time { return time.Date(2019, time.July, 22, 1, 0, 0, 0, time.UTC) } }) // 59 minute duration
	<-outputChannel
	metric = <-metricsChannel
	expectedMetric = Metric{Key: MetricsKeyOut, Value: 59 * time.Minute}
	if metric != expectedMetric {
		t.Errorf("unexpected metric for tract input expected: %+#v, recieved: %+#v", expectedMetric, metric)
	}

	input.setPreGetFunc(func() { now = func() time.Time { return time.Date(2019, time.July, 23, 0, 0, 0, 0, time.UTC) } }) // 23 hour duration
	close(inputChannel)
	metric = <-metricsChannel
	expectedMetric = Metric{Key: MetricsKeyIn, Value: 23 * time.Hour}
	if metric != expectedMetric {
		t.Errorf("unexpected metric for tract input expected: %+#v, recieved: %+#v", expectedMetric, metric)
	}
	waitOnTract()
}
