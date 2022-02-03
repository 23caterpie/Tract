package tract

import "context"

// WorkerFactory makes potentially many Worker objects that may use resources managed by the factory.
type WorkerFactory[InputType, OutputType Request, WorkerType Worker[InputType, OutputType]] interface {
	// MakeWorker makes a worker expected to run in a tract.
	// This Worker contructor will be called once per worker needed for a Worker Tract.
	// If a single worker needs its own resources that are contructed here and need closed when done,
	// the worker should implement Closer. the tract will call Close() when it shuts down.
	// Any resources the Workers will share should be instaniated and closed in a higher scope.
	MakeWorker() (WorkerType, error)
}

// Worker is an object that performs work potentially using it own resources and/or factory resources.
type Worker[InputType, OutputType Request] interface {
	// Work takes an input, performs an operation, and returns an output and potentially an error.
	// If the returned error is not nil, that specifies that the returned request will be discarded.
	Work(context.Context, InputType) (OutputType, error)
}

// Closer is something that closes its own locally scoped resources.
// Worker objects should implement this if their factory makes resources for them on construction.
type Closer interface {
	// Close closes resources
	Close()
}

var (
	_ WorkerFactory[int64, int64, Worker[int64, int64]] = workerAsFactory[int64, int64]{}
	_ Worker[int64, int64]                              = nonCloseWorker[int64, int64]{}
	_ Closer                                            = nonCloseWorker[int64, int64]{}
)

// NewFactoryFromWorker makes a WorkerFactory from a provided Worker.
// Whenever the WorkerFactory makes a worker, it just returns same worker
// it started with. This is useful for the common case of making a tract
// that uses workers who's Work() method is already thread safe without
// having to make a specific factory object. The worker will not be closed
// automatically.
func NewFactoryFromWorker[InputType, OutputType Request](
	worker Worker[InputType, OutputType],
) WorkerFactory[InputType, OutputType, Worker[InputType, OutputType]] {
	return workerAsFactory[InputType, OutputType]{
		worker: worker,
	}
}

type workerAsFactory[InputType, OutputType Request] struct {
	worker Worker[InputType, OutputType]
}

func (f workerAsFactory[InputType, OutputType]) MakeWorker() (Worker[InputType, OutputType], error) {
	return nonCloseWorker[InputType, OutputType]{f.worker}, nil
}

type nonCloseWorker[InputType, OutputType Request] struct {
	Worker[InputType, OutputType]
}

// This explicitely overrides the composed Worker's Close method if it has one.
func (f nonCloseWorker[InputType, OutputType]) Close() {}

// internalWorkerFactory stuff

func newInternalWorkerFactory[InputType, OutputType Request, WorkerType Worker[InputType, OutputType]](
	factory WorkerFactory[InputType, OutputType, WorkerType],
) WorkerFactory[InputType, OutputType, Worker[InputType, OutputType]] {
	return internalWorkerFactory[InputType, OutputType, WorkerType]{
		factory: factory,
	}
}

// internalWorkerFactory wraps a WorkerFactory to satisfy WorkerFactory[InputType, OutputType, Worker[InputType, OutputType]]
// to remove the WorkerType generic when we don't care what the actual type is.
type internalWorkerFactory[InputType, OutputType Request, WorkerType Worker[InputType, OutputType]] struct {
	factory WorkerFactory[InputType, OutputType, WorkerType]
}

func (f internalWorkerFactory[InputType, OutputType, WorkerType]) MakeWorker() (Worker[InputType, OutputType], error) {
	return f.factory.MakeWorker()
}
