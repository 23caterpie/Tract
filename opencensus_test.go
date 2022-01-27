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
)

var (
	customTag = tag.MustNewKey("my_test_custom_tag")
)

var (
	// Worker views

	testWorkerWorkLatencyView = &view.View{
		Name:        "octract.test/worker/work/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerWorkLatency,
		Aggregation: view.Count(),
	}
	testWorkerWaitLatencyView = &view.View{
		Name:        "octract.test/worker/wait/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerWaitLatency,
		Aggregation: view.Count(),
	}
	testWorkerInputLatencyView = &view.View{
		Name:        "octract.test/worker/input/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerInputLatency,
		Aggregation: view.Count(),
	}
	testWorkerOutputLatencyView = &view.View{
		Name:        "octract.test/worker/output/latency",
		TagKeys:     []tag.Key{tract.WorkerName, customTag},
		Measure:     tract.WorkerOutputLatency,
		Aggregation: view.Count(),
	}

	// Group views

	testGroupWorkLatencyView = &view.View{
		Name:        "octract.test/group/work/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupWorkLatency,
		Aggregation: view.Count(),
	}
	testGroupWaitLatencyView = &view.View{
		Name:        "octract.test/group/wait/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupWaitLatency,
		Aggregation: view.Count(),
	}
	testGroupInputLatencyView = &view.View{
		Name:        "octract.test/group/input/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupInputLatency,
		Aggregation: view.Count(),
	}
	testGroupOutputLatencyView = &view.View{
		Name:        "octract.test/group/output/latency",
		TagKeys:     []tag.Key{tract.GroupName, customTag},
		Measure:     tract.GroupOutputLatency,
		Aggregation: view.Count(),
	}
)

func init() {
	if err := view.Register(
		testWorkerWorkLatencyView,
		testWorkerWaitLatencyView,
		testWorkerInputLatencyView,
		testWorkerOutputLatencyView,
		testGroupWorkLatencyView,
		testGroupWaitLatencyView,
		testGroupInputLatencyView,
		testGroupOutputLatencyView,
	); err != nil {
		panic(err)
	}
}

func TestOpencensusMetrics(t *testing.T) {
	metricReader := metricexport.NewReader()
	metrics := test.NewExporter(metricReader)

	input := make(chan testOCRequest, 1)
	input <- testOCRequest{}
	close(input)

	const (
		workerName1      = `test-worker-1`
		workerName2      = `test-worker-2`
		serialGroupName1 = `test-serial-group-1`
		customTagValue   = "testaroonie-value"
	)

	myTract := tract.NewTractRunner[testOCRequest, testOCRequest](
		tract.NewChannel(input),
		tract.NewNamedLinker[testOCRequest, testOCRequest, testOCRequest](
			serialGroupName1,
			tract.NewWorkerTract(workerName1, 1,
				tract.NewFactoryFromWorker(newTestOCWorker()),
			),
		).Link(
			tract.NewWorkerTract(workerName2, 1,
				tract.NewFactoryFromWorker(newTestOCWorker()),
			),
		),
		newTestOCOutput(),
	)
	myTract.WithBaseContext = func(testOCRequest) context.Context {
		ctx, err := tag.New(context.Background(), tag.Upsert(customTag, customTagValue))
		assert.NoError(t, err)
		return ctx
	}
	assert.NoError(t, myTract.Run())

	var (
		worker1Tags      = map[string]string{tract.WorkerName.Name(): workerName1, customTag.Name(): customTagValue}
		worker2Tags      = map[string]string{tract.WorkerName.Name(): workerName2, customTag.Name(): customTagValue}
		serialGroup1Tags = map[string]string{tract.GroupName.Name(): serialGroupName1, customTag.Name(): customTagValue}
	)

	metrics.ReadAndExport()
	// assert.Equal(t, "2", metrics.String())

	// TODO: fix these:
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
	// serialGroup1 metrics
	assert.Equal(t, int64(1), getPoint(metrics, testGroupInputLatencyView.Name, serialGroup1Tags))
	assert.Equal(t, nil, getPoint(metrics, testGroupWaitLatencyView.Name, serialGroup1Tags)) // no wait metrics for the outer group since there is no previous output to start the wait.
	assert.Equal(t, int64(1), getPoint(metrics, testGroupWorkLatencyView.Name, serialGroup1Tags))
	assert.Equal(t, int64(1), getPoint(metrics, testGroupOutputLatencyView.Name, serialGroup1Tags))
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
