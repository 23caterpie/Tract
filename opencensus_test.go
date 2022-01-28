package tract_test

import (
	"context"
	"sync"

	tract "github.com/23caterpie/Tract"

	"go.opencensus.io/metric/test"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

var (
	customTag = tag.MustNewKey("my_test_custom_tag")
)

func resetTractMetrics() (
	*view.View,
	*view.View,
	*view.View,
	*view.View,
	*view.View,
	*view.View,
	*view.View,
	*view.View,
) {
	// Worker stats

	tract.WorkerWorkLatency = stats.Float64(
		"octract/worker/work/latency",
		"worker work latency",
		stats.UnitMilliseconds,
	)
	tract.WorkerWaitLatency = stats.Float64(
		"octract/worker/wait/latency",
		"worker wait latency",
		stats.UnitMilliseconds,
	)
	tract.WorkerInputLatency = stats.Float64(
		"octract/worker/input/latency",
		"worker input latency",
		stats.UnitMilliseconds,
	)
	tract.WorkerOutputLatency = stats.Float64(
		"octract/worker/output/latency",
		"worker output latency",
		stats.UnitMilliseconds,
	)

	// Group stats

	tract.GroupWorkLatency = stats.Float64(
		"octract/group/work/latency",
		"group work latency",
		stats.UnitMilliseconds,
	)
	tract.GroupWaitLatency = stats.Float64(
		"octract/group/wait/latency",
		"group wait latency",
		stats.UnitMilliseconds,
	)
	tract.GroupInputLatency = stats.Float64(
		"octract/group/input/latency",
		"group input latency",
		stats.UnitMilliseconds,
	)
	tract.GroupOutputLatency = stats.Float64(
		"octract/group/output/latency",
		"group output latency",
		stats.UnitMilliseconds,
	)

	// Worker views

	testWorkerWorkLatencyView := &view.View{
		Name:        "octract.test/worker/work/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerWorkLatency,
		Aggregation: view.Count(),
	}
	testWorkerWaitLatencyView := &view.View{
		Name:        "octract.test/worker/wait/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerWaitLatency,
		Aggregation: view.Count(),
	}
	testWorkerInputLatencyView := &view.View{
		Name:        "octract.test/worker/input/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerInputLatency,
		Aggregation: view.Count(),
	}
	testWorkerOutputLatencyView := &view.View{
		Name:        "octract.test/worker/output/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerOutputLatency,
		Aggregation: view.Count(),
	}

	// Group views

	testGroupWorkLatencyView := &view.View{
		Name:        "octract.test/group/work/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupWorkLatency,
		Aggregation: view.Count(),
	}
	testGroupWaitLatencyView := &view.View{
		Name:        "octract.test/group/wait/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupWaitLatency,
		Aggregation: view.Count(),
	}
	testGroupInputLatencyView := &view.View{
		Name:        "octract.test/group/input/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupInputLatency,
		Aggregation: view.Count(),
	}
	testGroupOutputLatencyView := &view.View{
		Name:        "octract.test/group/output/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupOutputLatency,
		Aggregation: view.Count(),
	}

	return testWorkerWorkLatencyView,
		testWorkerWaitLatencyView,
		testWorkerInputLatencyView,
		testWorkerOutputLatencyView,
		testGroupWorkLatencyView,
		testGroupWaitLatencyView,
		testGroupInputLatencyView,
		testGroupOutputLatencyView
}

type testOCRequest struct{}

func newTestOCWorker() tract.Worker[testOCRequest, testOCRequest] {
	return testOCWorker{}
}

type testOCWorker struct{}

func (w testOCWorker) Work(ctx context.Context, r testOCRequest) (testOCRequest, bool) {
	return r, true
}

func newTestOCOutput() tract.Output[testOCRequest] {
	return testOCOutput{}
}

type testOCOutput struct{}

func (testOCOutput) Close()            {}
func (testOCOutput) Put(testOCRequest) {}

func getPoint(metrics *test.Exporter, viewName string, labels map[string]string) any {
	point, ok := metrics.GetPoint(viewName, labels)
	if !ok {
		return nil
	}
	return point.Value
}

func newTestTraceExporter() testTraceExporter {
	return testTraceExporter{
		mutex: &sync.Mutex{},
	}
}

type testTraceExporter struct {
	spans []*trace.SpanData
	mutex *sync.Mutex
}

func (e *testTraceExporter) ExportSpan(sd *trace.SpanData) {
	e.mutex.Lock()
	e.spans = append(e.spans, sd)
	e.mutex.Unlock()
}
