package tract

// WorkerFactory makes potentially many Worker objects that may use resources managed by the factory.
type WorkerFactory[InputType, OutputType any] interface {
	// MakeWorker makes a worker expected to run in a tract.
	// This Worker contructor will be called once per worker needed for a Worker Tract.
	// Any resources that a single worker will need (and not share with other Workers) should be
	// instanciated here, and closed by the Worker's Close() method. Any resources the Workers will
	// share should be instantiated in this WorkerFactory's Contructor and closed by its Close()
	// method, or should be instaniated and closed in a higher scope.
	MakeWorker() (WorkerCloser[InputType, OutputType], error)
}

// Worker is an object that performs work potentially using it own resources and/or factory resources.
type Worker[InputType, OutputType any] interface {
	// Work takes a request, performs an operation, and returns that request and a success flag.
	// If the returned bool is false, that specifies that the returned request should be discarded.
	// The expected pattern is to retrieve any needed arguments from the request using request.Value(...)
	// then apply the results of the work to the same request using context.WithValue(request, ...).
	// When designing workers keep the keys for the request values you will be using in mind.
	Work(InputType) (OutputType, bool)
}

// WorkerCloser is a Worker that closes its own locally scoped resources.
type WorkerCloser[InputType, OutputType any] interface {
	Worker[InputType, OutputType]
	// Close closes worker resources
	Close()
}

var (
	_ WorkerFactory[int64, int64] = workerAsFactory[int64, int64]{}
	_ WorkerCloser[int64, int64]  = nonCloseWorker[int64, int64]{}
)

// NewFactoryFromWorker makes a WorkerFactory from a provided Worker.
// Whenever the WorkerFactory makes a worker, it just returns same worker
// it started with. This is useful for the common case of making a tract
// that uses workers who's Work() function is already thred safe. without
// having to make a specific factory object. The worker's call to close is
// defered until the factory is closed.
func NewFactoryFromWorker[InputType, OutputType any](worker Worker[InputType, OutputType]) WorkerFactory[InputType, OutputType] {
	return workerAsFactory[InputType, OutputType]{
		worker: worker,
	}
}

type workerAsFactory[InputType, OutputType any] struct {
	worker Worker[InputType, OutputType]
}

func (f workerAsFactory[InputType, OutputType]) MakeWorker() (WorkerCloser[InputType, OutputType], error) {
	return nonCloseWorker[InputType, OutputType]{f.worker}, nil
}

type nonCloseWorker[InputType, OutputType any] struct {
	Worker[InputType, OutputType]
}

func (f nonCloseWorker[InputType, OutputType]) Close() {}
