package tract_test

import (
	"context"
	"testing"

	tract "github.com/23caterpie/Tract"

	"github.com/stretchr/testify/assert"
	"go.opencensus.io/metric/metricexport"
	"go.opencensus.io/metric/test"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

func TestOpencensusLinkerTract(t *testing.T) {
	// Start *Setup Metrics*
	testWorkerWorkLatencyView,
		testWorkerWaitLatencyView,
		testWorkerInputLatencyView,
		testWorkerOutputLatencyView,
		testGroupWorkLatencyView,
		testGroupWaitLatencyView,
		testGroupInputLatencyView,
		testGroupOutputLatencyView := resetTractMetrics()
	views := []*view.View{
		testWorkerWorkLatencyView,
		testWorkerWaitLatencyView,
		testWorkerInputLatencyView,
		testWorkerOutputLatencyView,
		testGroupWorkLatencyView,
		testGroupWaitLatencyView,
		testGroupInputLatencyView,
		testGroupOutputLatencyView,
	}
	assert.NoError(t, view.Register(views...))
	defer view.Unregister(views...)

	metricReader := metricexport.NewReader()
	metrics := test.NewExporter(metricReader)
	// End *Setup Metrics*

	// Start *Setup Tracing*
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	traceExporter := newTestTraceExporter()
	trace.RegisterExporter(&traceExporter)
	// End *Setup Tracing*

	input := make(chan testOCRequest, 1)
	input <- testOCRequest{}
	close(input)

	const (
		workerName1      = `test-worker-1`
		workerName2      = `test-worker-2`
		workerName3      = `test-worker-3`
		serialGroupName1 = `test-serial-group-1`
		customTagValue   = "testaroonie-value"
	)

	tractRunner := tract.NewTractRunner[testOCRequest, testOCRequest](
		tract.NewChannel(input),
		tract.NewNamedLinker[testOCRequest, testOCRequest, testOCRequest](
			serialGroupName1,
			tract.NewWorkerTract(workerName1, 1, newTestOCWorker()),
		).Link(tract.NewLinker[testOCRequest, testOCRequest, testOCRequest](
			tract.NewWorkerTract(workerName2, 1, newTestOCWorker()),
		).Link(
			tract.NewWorkerTract(workerName3, 1, newTestOCWorker()),
		)),
		newTestOCOutput(),
	)
	tractRunner.WithBaseContext = func(testOCRequest) context.Context {
		ctx, err := tag.New(context.Background(), tag.Upsert(customTag, customTagValue))
		assert.NoError(t, err)
		return ctx
	}
	assert.NoError(t, tractRunner.Run())

	var (
		worker1Tags      = map[string]string{tract.WorkerName.Name(): workerName1, customTag.Name(): customTagValue}
		worker2Tags      = map[string]string{tract.WorkerName.Name(): workerName2, customTag.Name(): customTagValue}
		worker3Tags      = map[string]string{tract.WorkerName.Name(): workerName3, customTag.Name(): customTagValue}
		serialGroup1Tags = map[string]string{tract.GroupName.Name(): serialGroupName1, customTag.Name(): customTagValue}
	)

	// Test Metrics
	metrics.ReadAndExport()
	// assert.Equal(t, "uncomment to see things", metrics.String())

	// worker1 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker1Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker1Tags)) // no wait metrics for the first worker since there is no previous output to start the wait.
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerWorkLatencyView.Name, worker1Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerOutputLatencyView.Name, worker1Tags))
	// worker2 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker2Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerWaitLatencyView.Name, worker2Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerWorkLatencyView.Name, worker2Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerOutputLatencyView.Name, worker2Tags))
	// worker3 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker3Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerWaitLatencyView.Name, worker3Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerWorkLatencyView.Name, worker3Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerOutputLatencyView.Name, worker3Tags))
	// serialGroup1 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testGroupInputLatencyView.Name, serialGroup1Tags))
	assert.Equal(t, nil, getPoint(metrics, testGroupWaitLatencyView.Name, serialGroup1Tags)) // no wait metrics for the outer group since there is no previous output to start the wait.
	assert.Equal(t, int64(1), getPoint(metrics, testGroupWorkLatencyView.Name, serialGroup1Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testGroupOutputLatencyView.Name, serialGroup1Tags))

	// Test Traces
	assert.Len(t, traceExporter.spans, 8)
	spansByName := map[string]*trace.SpanData{}
	for _, span := range traceExporter.spans {
		spansByName[span.Name] = span
	}
	assert.Len(t, spansByName, 8)
	serialGroup1WorkSpan := spansByName["octract/group/test-serial-group-1/work"]
	worker1WorkSpan := spansByName["octract/worker/test-worker-1/work"]
	worker1WaitSpan := spansByName["octract/worker/test-worker-1/wait"]
	worker2WorkSpan := spansByName["octract/worker/test-worker-2/work"]
	worker2WaitSpan := spansByName["octract/worker/test-worker-2/wait"]
	worker3WorkSpan := spansByName["octract/worker/test-worker-3/work"]
	worker3WaitSpan := spansByName["octract/worker/test-worker-3/wait"]
	serialGroup1WaitSpan := spansByName["octract/group/test-serial-group-1/wait"]

	var serialGroup1WorkSpanID string
	// Group spans
	if assert.NotNil(t, serialGroup1WorkSpan) && assert.NotNil(t, serialGroup1WaitSpan) {
		// Test these spans are siblings with no parent.
		const rootSpanID = "0000000000000000"
		assert.Equal(t, rootSpanID, serialGroup1WorkSpan.ParentSpanID.String())
		assert.Equal(t, rootSpanID, serialGroup1WaitSpan.ParentSpanID.String())
		// Test these sibling spans were born in the right order.
		assert.True(t, serialGroup1WorkSpan.StartTime.Before(serialGroup1WaitSpan.StartTime), "serial group tract work must start before its wait starts")
		// Test for children.
		assert.Equal(t, 6, serialGroup1WorkSpan.ChildSpanCount)
		assert.Equal(t, 0, serialGroup1WaitSpan.ChildSpanCount)
		// Assign the work span for its children's tests.
		serialGroup1WorkSpanID = serialGroup1WorkSpan.SpanID.String()
	}
	// Sibling worker spans
	if assert.NotNil(t, worker1WorkSpan) && assert.NotNil(t, worker1WaitSpan) &&
		assert.NotNil(t, worker2WorkSpan) && assert.NotNil(t, worker2WaitSpan) &&
		assert.NotNil(t, worker3WorkSpan) && assert.NotNil(t, worker3WaitSpan) {
		// Test these spans are siblings with the serial work span as their parent.
		assert.Equal(t, serialGroup1WorkSpanID, worker1WorkSpan.ParentSpanID.String())
		assert.Equal(t, serialGroup1WorkSpanID, worker1WaitSpan.ParentSpanID.String())
		assert.Equal(t, serialGroup1WorkSpanID, worker2WorkSpan.ParentSpanID.String())
		assert.Equal(t, serialGroup1WorkSpanID, worker2WaitSpan.ParentSpanID.String())
		assert.Equal(t, serialGroup1WorkSpanID, worker3WorkSpan.ParentSpanID.String())
		assert.Equal(t, serialGroup1WorkSpanID, worker3WaitSpan.ParentSpanID.String())
		// Test these sibling spans were born in the right order.
		assert.True(t, worker1WorkSpan.StartTime.Before(worker1WaitSpan.StartTime), "worker tract work must start before its wait starts")
		assert.True(t, worker1WaitSpan.StartTime.Before(worker2WorkSpan.StartTime), "worker tract wait must start before the next worker work starts")
		assert.True(t, worker2WorkSpan.StartTime.Before(worker2WaitSpan.StartTime), "worker tract work must start before its wait starts")
		assert.True(t, worker2WaitSpan.StartTime.Before(worker3WorkSpan.StartTime), "worker tract wait must start before the next worker work starts")
		assert.True(t, worker3WorkSpan.StartTime.Before(worker3WaitSpan.StartTime), "worker tract work must start before its wait starts")
		// Test for no children.
		assert.Equal(t, 0, worker1WorkSpan.ChildSpanCount)
		assert.Equal(t, 0, worker1WaitSpan.ChildSpanCount)
		assert.Equal(t, 0, worker2WorkSpan.ChildSpanCount)
		assert.Equal(t, 0, worker2WaitSpan.ChildSpanCount)
		assert.Equal(t, 0, worker3WorkSpan.ChildSpanCount)
		assert.Equal(t, 0, worker3WaitSpan.ChildSpanCount)
	}
}
