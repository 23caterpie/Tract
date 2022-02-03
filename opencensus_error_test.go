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

func TestOpencensusErrorTract(t *testing.T) {
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
		serialGroupName1 = `test-serial-group-1`
		customTagValue   = "testaroonie-value"
	)

	tractRunner := tract.NewTractRunner[testOCRequest, testOCRequest](
		tract.NewChannel(input),
		tract.NewNamedLinker[testOCRequest, testOCRequest, testOCRequest](
			serialGroupName1,
			tract.NewWorkerTract(workerName1, 1, newTestOCErrorWorker()),
		).Link(
			tract.NewWorkerTract(workerName2, 1, newTestOCWorker()),
		),
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
		serialGroup1Tags = map[string]string{tract.GroupName.Name(): serialGroupName1, customTag.Name(): customTagValue}
		baseGroupTags    = map[string]string{tract.GroupName.Name(): "octract/base", customTag.Name(): customTagValue}
	)

	// Test Metrics
	metrics.ReadAndExport()
	// assert.Equal(t, "uncomment to see things", metrics.String())

	// worker1 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker1Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker1Tags))   // no wait metrics for the first worker since there is no previous output to start the wait.
	assert.Equal(t, nil, getPoint(metrics, testWorkerWorkLatencyView.Name, worker1Tags))   // early out request did contribute to this
	assert.Equal(t, nil, getPoint(metrics, testWorkerOutputLatencyView.Name, worker1Tags)) // early out request did not contribute to this
	// worker2 metrics
	assert.Equal(t, nil, getPoint(metrics, testWorkerInputLatencyView.Name, worker2Tags))  // early out request never made it to worker2
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker2Tags))   // early out request never made it to worker2
	assert.Equal(t, nil, getPoint(metrics, testWorkerWorkLatencyView.Name, worker2Tags))   // early out request never made it to worker2
	assert.Equal(t, nil, getPoint(metrics, testWorkerOutputLatencyView.Name, worker2Tags)) // early out request never made it to worker2
	// serialGroup1 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testGroupInputLatencyView.Name, serialGroup1Tags))
	assert.Equal(t, nil, getPoint(metrics, testGroupWaitLatencyView.Name, serialGroup1Tags))   // no wait metrics for the outer group since there is no previous output to start the wait.
	assert.Equal(t, nil, getPoint(metrics, testGroupWorkLatencyView.Name, serialGroup1Tags))   // early out request did contribute to this
	assert.Equal(t, nil, getPoint(metrics, testGroupOutputLatencyView.Name, serialGroup1Tags)) // early out request did contribute to this
	// base metrics
	assert.Equal(t, nil, getPoint(metrics, testGroupInputLatencyView.Name, baseGroupTags)) // no input metrics for base group.
	assert.Equal(t, nil, getPoint(metrics, testGroupWaitLatencyView.Name, baseGroupTags))  // no wait metrics for base group.
	assert.Equal(t, int64(1), getPoint(metrics, testGroupWorkLatencyView.Name, baseGroupTags))
	assert.Equal(t, nil, getPoint(metrics, testGroupOutputLatencyView.Name, baseGroupTags)) // no output metrics for base group.

	// Test Traces
	assert.Len(t, traceExporter.spans, 3)
	spansByName := map[string]*trace.SpanData{}
	var traceID string
	for i, span := range traceExporter.spans {
		spansByName[span.Name] = span
		if i == 0 {
			traceID = span.TraceID.String()
		} else {
			assert.Equal(t, traceID, span.TraceID.String(), "all spans must share the same trace id")
		}
	}
	assert.Len(t, spansByName, 3)
	baseSpan := spansByName["octract/base"]
	serialGroup1WorkSpan := spansByName["octract/group/test-serial-group-1/work"]
	worker1WorkSpan := spansByName["octract/worker/test-worker-1/work"]
	worker1WaitSpan := spansByName["octract/worker/test-worker-1/wait"]
	worker2WorkSpan := spansByName["octract/worker/test-worker-2/work"]
	worker2WaitSpan := spansByName["octract/worker/test-worker-2/wait"]
	serialGroup1WaitSpan := spansByName["octract/group/test-serial-group-1/wait"]

	assert.Nil(t, serialGroup1WaitSpan)
	assert.Nil(t, worker1WaitSpan)
	assert.Nil(t, worker2WorkSpan)
	assert.Nil(t, worker2WaitSpan)

	var baseSpanID string
	if assert.NotNil(t, baseSpan) {
		// Test the base span has no parent.
		const rootSpanID = "0000000000000000"
		assert.Equal(t, rootSpanID, baseSpan.ParentSpanID.String())
		// Test for children.
		assert.Equal(t, 1, baseSpan.ChildSpanCount)
		// Assign the base span for its children's tests.
		baseSpanID = baseSpan.SpanID.String()
	}

	var serialGroup1WorkSpanID string
	// Group spans
	if assert.NotNil(t, serialGroup1WorkSpan) {
		// Test this spans has the base parent.
		assert.Equal(t, baseSpanID, serialGroup1WorkSpan.ParentSpanID.String())
		// Test for children.
		assert.Equal(t, 1, serialGroup1WorkSpan.ChildSpanCount)
		// Assign the work span for its children's tests.
		serialGroup1WorkSpanID = serialGroup1WorkSpan.SpanID.String()
	}
	// Sibling worker spans
	if assert.NotNil(t, worker1WorkSpan) {
		// Test this span has the serial work span as its parent.
		assert.Equal(t, serialGroup1WorkSpanID, worker1WorkSpan.ParentSpanID.String())
		// Test for no children.
		assert.Equal(t, 0, worker1WorkSpan.ChildSpanCount)
	}
}
