package tract

import "sync"

// WorkerFactory makes potentially many Worker objects that may use resources managed by the factory.
type WorkerFactory[T any] interface {
	// MakeWorker makes a worker expected to run in a tract.
	// This Worker contructor will be called once per worker needed for a Worker Tract.
	// Any resources that a single worker will need (and not share with other Workers) should be
	// instanciated here, and closed by the Worker's Close() method. Any resources the Workers will
	// share should be instantiated in this WorkerFactory's Contructor and closed by its Close()
	// method, or should be instaniated and closed in a higher scope.
	MakeWorker() (Worker[T], error)
	// Close closes factory resources
	Close()
}

// Worker is an object that performs work potentially using it own resources and/or factory resources.
type Worker[T any] interface {
	// Work takes a request, performs an operation, and returns that request and a success flag.
	// If the returned bool is false, that specifies that the returned request should be discarded.
	// The expected pattern is to retrieve any needed arguments from the request using request.Value(...)
	// then apply the results of the work to the same request using context.WithValue(request, ...).
	// When designing workers keep the keys for the request values you will be using in mind.
	Work(Request[T]) (Request[T], bool)
	// Close closes worker resources
	Close()
}

var (
	_ WorkerFactory[int64] = &tractWorkerFactory[int64]{}
	_ WorkerFactory[int64] = workerAsFactory[int64]{}

	_ Worker[int64] = MetricsWorker[int64]{}
	_ Worker[int64] = tractWorker[int64]{}
)

// NewFactoryFromWorker makes a WorkerFactory from a provided Worker.
// Whenever the WorkerFactory makes a worker, it just returns same worker
// it started with. This is useful for the common case of making a tract
// that uses workers who's Work() function is already thred safe. without
// having to make a specific factory object. The worker's call to close is
// defered until the factory is closed.
func NewFactoryFromWorker[T any](worker Worker[T]) WorkerFactory[T] {
	return workerAsFactory[T]{worker: worker}
}

type workerAsFactory[T any] struct {
	worker Worker[T]
}

func (f workerAsFactory[T]) MakeWorker() (Worker[T], error) {
	return nonCloseWorker[T]{f.worker}, nil
}

func (f workerAsFactory[_]) Close() {
	f.worker.Close()
}

type nonCloseWorker[T any] struct {
	Worker[T]
}

func (f nonCloseWorker[_]) Close() {}

// MetricsWorker is a wrapper around a Worker that will automatically generate during latency metrics.
type MetricsWorker[T any] struct {
	Worker[T]
	metricsHandler MetricsHandler
}

// Work works using the inner Worker while gathering metrics.
func (w MetricsWorker[T]) Work(r Request[T]) (Request[T], bool) {
	var (
		request Request[T]
		ok      bool
	)
	if w.metricsHandler != nil && w.metricsHandler.ShouldHandle() {
		before := now()
		request, ok = w.Worker.Work(r)
		after := now()
		w.metricsHandler.HandleMetrics(
			Metric{MetricsKeyDuring, after.Sub(before)},
		)
	} else {
		request, ok = w.Worker.Work(r)
	}
	return request, ok
}

// NewTractWorkerFactory turns a Tract into a WorkerFactory.
// When it makes it's first worker, it initializes and starts the tract.
// Its workers work consist of passing requests into the tract, waiting
// for the request to reach the end of the tract, and returning the
// resulting request. Any cleanups put on the request in the tract, versus
// cleanups put on the request called before it reaches this worker are kept
// entirely separated. This tract's cleanups occur at the end of just itself;
// cleanups put on the request before hand will occur when they normally would
// have.
func NewTractWorkerFactory[T any](tract Tract[T]) WorkerFactory[T] {
	return &tractWorkerFactory[T]{
		Tract:      tract,
		tractInput: make(chan Request[T]),
	}
}

type tractWorkerFactory[T any] struct {
	Tract[T]
	tractInput     chan Request[T]
	tractClosure   func()
	startTractOnce sync.Once
}

func (w *tractWorkerFactory[T]) MakeWorker() (Worker[T], error) {
	var err error
	w.startTractOnce.Do(func() {
		w.Tract.SetInput(InputChannel[T](w.tractInput))
		err = w.Tract.Init()
		if err != nil {
			return
		}
		w.tractClosure = w.Tract.Start()
	})
	if err != nil {
		return nil, err
	}
	return tractWorker[T]{
		in: w.tractInput,
	}, nil
}

func (w *tractWorkerFactory[_]) Close() {
	close(w.tractInput)
	if w.tractClosure != nil {
		w.tractClosure()
	}
	w.startTractOnce = sync.Once{}
}

type tractWorker[T any] struct {
	in chan Request[T]
}

func (w tractWorker[T]) Work(req Request[T]) (Request[T], bool) {
	type requestSuccessTuple struct {
		request Request[T]
		success bool
	}
	var (
		deferedCleanups cleanups[T]
		returnChannel   = make(chan requestSuccessTuple)
	)
	// Save the request cleanups for later. We do not want these cleanups to activate at the end of the tract
	// we are sending this request down. Instead we will use this tract's cleanup to return it to us.
	deferedCleanups = swapCleanups(&req, cleanups[T]{func(r Request[T], success bool) {
		returnChannel <- requestSuccessTuple{
			request: r,
			success: success,
		}
	}})
	w.in <- req
	// Wait for the request to reach the end of the tract we sent it down where it will be cleaned up and sent back here.
	postWorkTractRequest := <-returnChannel
	_ = swapCleanups(&postWorkTractRequest.request, deferedCleanups)
	return postWorkTractRequest.request, postWorkTractRequest.success
}

func (w tractWorker[_]) Close() {}
