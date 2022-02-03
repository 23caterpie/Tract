package tract_test

import (
	"context"
	"math"
	"sync"

	tract "github.com/23caterpie/Tract"
)

// These are compiler checks to make sure our implementations satisfy the tract Workers interface
var (
	_ tract.Input[float64]           = NewSliceArgReaderInput([]float64{1, 2, 3})
	_ tract.Worker[float64, float64] = SquareRootWorker{}
	_ tract.Output[float64]          = &SliceResultsWriterOutput[float64]{}
)

// NewSquareRootWorker returns a SquareRootWorker as a tract.Worker type more ready for type inference.
func NewSquareRootWorker() tract.Worker[float64, float64] {
	return SquareRootWorker{}
}

// SquareRootWorker is a middle stage worker in a tract.
// Middle stage workers typically will get arguments from the request's Data field
// then perfrom an operation, and store the results back to the same request's Data field.
// SquareRootWorker performs `math.Sqrt` on its argument.
type SquareRootWorker struct{}

func (w SquareRootWorker) Work(_ context.Context, r float64) (float64, error) {
	return math.Sqrt(r), nil
}

func (w SquareRootWorker) Close() {}

func NewSliceArgReaderInput[T any](args []T) *SliceArgReaderInput[T] {
	return &SliceArgReaderInput[T]{
		arguments: args,
		mutex:     sync.Mutex{},
	}
}

// SliceArgReaderInput is first stage of the tract.
// First stage workers typically will generate or retrive data from some sort of source,
// such as a queue, file, or user provided data, and store that data on the request's Data field.
// From here later stages of the tract can use this data. The first stage is also responsible
// for comencing shutdown of the entire tract. When its call to `Work()` returns a false bool,
// it will signal a shutdown of the entire tract. Thus head workers should return false when
// they are done pulling in data from the source.
// SliceArgReaderInput uses its user populated `arguments` field as a queue. Each time `Work`
// is called it pops an item off of `arguments` and passed it on the tract Request.
// When there are no more items, false is returned.
// Since SliceArgReaderInput is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceArgReaderInput[T any] struct {
	arguments []T
	mutex     sync.Mutex
}

func (w *SliceArgReaderInput[T]) Get() (T, bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	var output T
	if len(w.arguments) == 0 {
		return output, false
	}
	output, w.arguments = w.arguments[0], w.arguments[1:]
	return output, true
}

// SliceResultsWriterOutput is the last stage of the tract.
// Last stage workers typically perform the final operations on a Request
// SliceResultsWriterOutput gets the data from the request and pushes it onto a list of results.
// Since SliceResultsWriterOutput is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceResultsWriterOutput[T any] struct {
	results []T
	mutex   sync.Mutex
}

func (w *SliceResultsWriterOutput[T]) Put(t T) {
	w.mutex.Lock()
	w.results = append(w.results, t)
	w.mutex.Unlock()
}

func (w *SliceResultsWriterOutput[T]) Close() {}
