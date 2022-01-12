package octract

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

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
		TagKeys:     []tag.Key{WorkerName, customTag},
		Measure:     WorkerWorkLatency,
		Aggregation: view.Sum(),
	}
	testWorkerWaitLatencyView = &view.View{
		Name:        "octract.test/worker/wait/latency",
		TagKeys:     []tag.Key{WorkerName, customTag},
		Measure:     WorkerWaitLatency,
		Aggregation: view.Sum(),
	}
	testWorkerInputLatencyView = &view.View{
		Name:        "octract.test/worker/input/latency",
		TagKeys:     []tag.Key{WorkerName, customTag},
		Measure:     WorkerInputLatency,
		Aggregation: view.Sum(),
	}
	testWorkerOutputLatencyView = &view.View{
		Name:        "octract.test/worker/output/latency",
		TagKeys:     []tag.Key{WorkerName, customTag},
		Measure:     WorkerOutputLatency,
		Aggregation: view.Sum(),
	}

	// Group views

	testGroupWorkLatencyView = &view.View{
		Name:        "octract.test/group/work/latency",
		TagKeys:     []tag.Key{GroupName, customTag},
		Measure:     GroupWorkLatency,
		Aggregation: view.Sum(),
	}
)

func init() {
	if err := view.Register(
		testWorkerWorkLatencyView,
		testWorkerWaitLatencyView,
		testWorkerInputLatencyView,
		testWorkerOutputLatencyView,
		testGroupWorkLatencyView,
	); err != nil {
		panic(err)
	}
}

func TestWorkerBasicRequest(t *testing.T) {
	metricReader := metricexport.NewReader()
	metrics := test.NewExporter(metricReader)

	input := make(chan basicRequest, 1)
	input <- basicRequest{}
	close(input)

	work1Done := make(chan struct{})
	work2Stopper := make(chan struct{})

	var (
		workerName1 = `basic-test1`
		workerName2 = `basic-test2`
	)
	setSince(1 * time.Millisecond)

	g := &errgroup.Group{}
	g.Go(func() error {
		return tract.Run[basicRequest, basicRequest](
			tract.Channel[basicRequest](input),
			tract.NewLinker[basicRequest, basicRequest, basicRequest](
				tract.NewWorkerTract[basicRequest, basicRequest](workerName1, 1,
					tract.NewFactoryFromWorker[basicRequest, basicRequest](testWorker[basicRequest]{f: func() {
						setSince(10 * time.Millisecond)
					}}),
				),
			).Link(
				tract.NewWorkerTract[basicRequest, basicRequest](workerName2, 1,
					tract.NewFactoryFromWorker[basicRequest, basicRequest](testWorker[basicRequest]{f: func() {
						close(work1Done)
						<-work2Stopper
						setSince(100 * time.Millisecond)
					}}),
				),
			),
			noopOutput[basicRequest]{},
		)
	})

	var (
		worker1Tags = map[string]string{WorkerName.Name(): workerName1}
		worker2Tags = map[string]string{WorkerName.Name(): workerName2}
	)

	// Export metrics after worker1 has output to worker2 but before worker2 has finished.
	<-work1Done
	metrics.ReadAndExport()
	// assert.Equal(t, "1", metrics.String())
	// worker1 metrics
	assert.Equal(t, float64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker1Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerWorkLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerOutputLatencyView.Name, worker1Tags))
	// worker2 metrics
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerInputLatencyView.Name, worker2Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker2Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWorkLatencyView.Name, worker2Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerOutputLatencyView.Name, worker2Tags))

	// Export metrics after entire tract has finished.
	close(work2Stopper)
	assert.NoError(t, g.Wait())
	metrics.ReadAndExport()
	// assert.Equal(t, "2", metrics.String())

	// worker1 metrics
	assert.Equal(t, float64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker1Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerWorkLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerOutputLatencyView.Name, worker1Tags))
	// worker2 metrics
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerInputLatencyView.Name, worker2Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker2Tags))
	assert.Equal(t, float64(100), getPoint(metrics, testWorkerWorkLatencyView.Name, worker2Tags))
	assert.Equal(t, float64(100), getPoint(metrics, testWorkerOutputLatencyView.Name, worker2Tags))
}

func TestWorkerContextRequest(t *testing.T) {
	metricReader := metricexport.NewReader()
	metrics := test.NewExporter(metricReader)

	input := make(chan *contextRequest, 1)
	input <- newContextRequest()
	close(input)

	work1Done := make(chan struct{})
	work2Stopper := make(chan struct{})

	var (
		workerName1 = `context-test1`
		workerName2 = `context-test2`
	)
	setSince(1 * time.Millisecond)

	g := &errgroup.Group{}
	g.Go(func() error {
		return tract.Run[*contextRequest, *contextRequest](
			tract.Channel[*contextRequest](input),
			tract.NewLinker[*contextRequest, *contextRequest, *contextRequest](
				tract.NewWorkerTract[*contextRequest, *contextRequest](workerName1, 1,
					tract.NewFactoryFromWorker[*contextRequest, *contextRequest](testWorker[*contextRequest]{f: func() {
						setSince(10 * time.Millisecond)
					}}),
				),
			).Link(
				tract.NewWorkerTract[*contextRequest, *contextRequest](workerName2, 1,
					tract.NewFactoryFromWorker[*contextRequest, *contextRequest](testWorker[*contextRequest]{f: func() {
						close(work1Done)
						<-work2Stopper
						setSince(100 * time.Millisecond)
					}}),
				),
			),
			noopOutput[*contextRequest]{},
		)
	})

	var (
		worker1Tags = map[string]string{WorkerName.Name(): workerName1}
		worker2Tags = map[string]string{WorkerName.Name(): workerName2}
	)

	// Export metrics after worker1 has output to worker2 but before worker2 has finished.
	<-work1Done
	metrics.ReadAndExport()
	// assert.Equal(t, "1", metrics.String())
	// worker1 metrics
	assert.Equal(t, float64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker1Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerWorkLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerOutputLatencyView.Name, worker1Tags))
	// worker2 metrics
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerInputLatencyView.Name, worker2Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerWaitLatencyView.Name, worker2Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWorkLatencyView.Name, worker2Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerOutputLatencyView.Name, worker2Tags))

	// Export metrics after entire tract has finished.
	close(work2Stopper)
	assert.NoError(t, g.Wait())
	metrics.ReadAndExport()
	// assert.Equal(t, "2", metrics.String())

	// worker1 metrics
	assert.Equal(t, float64(1), getPoint(metrics, testWorkerInputLatencyView.Name, worker1Tags))
	assert.Equal(t, nil, getPoint(metrics, testWorkerWaitLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerWorkLatencyView.Name, worker1Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerOutputLatencyView.Name, worker1Tags))
	// worker2 metrics
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerInputLatencyView.Name, worker2Tags))
	assert.Equal(t, float64(10), getPoint(metrics, testWorkerWaitLatencyView.Name, worker2Tags))
	assert.Equal(t, float64(100), getPoint(metrics, testWorkerWorkLatencyView.Name, worker2Tags))
	assert.Equal(t, float64(100), getPoint(metrics, testWorkerOutputLatencyView.Name, worker2Tags))
}

var (
	sinceSetterMutex = &sync.Mutex{}
)

func setSince(d time.Duration) {
	sinceSetterMutex.Lock()
	defer sinceSetterMutex.Unlock()
	since = func(time.Time) time.Duration {
		sinceSetterMutex.Lock()
		defer sinceSetterMutex.Unlock()
		return d
	}
}

type basicRequest struct{}

// TODO: don't like this, shouldn't need to be a pointer, I want requests to copy when they split.
var _ tract.ContextRequest = &contextRequest{}

func newContextRequest() *contextRequest {
	return &contextRequest{
		ctx:      context.Background(),
		ctxMutex: &sync.Mutex{},
	}
}

type contextRequest struct {
	ctx      context.Context
	ctxMutex *sync.Mutex
}

func (r *contextRequest) Context() (context.Context, func(context.Context)) {
	r.ctxMutex.Lock()
	return r.ctx, func(ctx context.Context) {
		defer r.ctxMutex.Unlock()
		r.ctx = ctx
	}
}

type testWorker[T any] struct {
	f func()
}

func (w testWorker[T]) Work(t T) (T, bool) {
	w.f()
	return t, true
}

type noopOutput[T any] struct{}

func (noopOutput[T]) Close() {}
func (noopOutput[T]) Put(T)  {}

func getPoint(metrics *test.Exporter, viewName string, labels map[string]string) any {
	point, ok := metrics.GetPoint(viewName, labels)
	if !ok {
		return nil
	}
	return point.Value
}
