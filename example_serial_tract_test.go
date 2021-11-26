package tract_test

import (
	"fmt"
	"math"
	"sort"
	"sync"

	tract "github.com/23caterpie/Tract"
)

// These are compiler checks to make sure our implementations satisfy the tract Workers interface
var (
	_ tract.Input[float64]           = &SliceArgReaderInput{}
	_ tract.Worker[float64, float64] = SquareRootWorker{}
	_ tract.Output[float64]          = &SliceResultsWriterOutput{}
)

// SquareRootWorker is a middle stage worker in a tract.
// Middle stage workers typically will get arguments from the request's Data field
// then perfrom an operation, and store the results back to the same request's Data field.
// SquareRootWorker performs `math.Sqrt` on its argument.
type SquareRootWorker struct{}

func (w SquareRootWorker) Work(r float64) (float64, bool) {
	return math.Sqrt(r), true
}

func (w SquareRootWorker) Close() {}

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
type SliceArgReaderInput struct {
	arguments []float64
	mutex     sync.Mutex
}

func (w *SliceArgReaderInput) Get() (float64, bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if len(w.arguments) == 0 {
		return 0, false
	}
	var output float64
	output, w.arguments = w.arguments[0], w.arguments[1:]
	return output, true
}

// SliceResultsWriterOutput is the last stage of the tract.
// Last stage workers typically perform the final operations on a Request
// SliceResultsWriterOutput gets the data from the request and pushes it onto a list of results.
// Since SliceResultsWriterOutput is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceResultsWriterOutput struct {
	results []float64
	mutex   sync.Mutex
}

func (w *SliceResultsWriterOutput) Put(r float64) {
	w.mutex.Lock()
	w.results = append(w.results, r)
	w.mutex.Unlock()
}

func (w *SliceResultsWriterOutput) Close() {}

// Perpare a few numbers to be square rooted and do it using a tract!
func ExampleTract_serialGroupTract() {
	var (
		input  = &SliceArgReaderInput{arguments: []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100}}
		output SliceResultsWriterOutput
	)

	err := tract.Run[float64, float64](
		input,
		tract.NewWorkerTract("square root", 4,
			tract.NewFactoryFromWorker[float64, float64](SquareRootWorker{}),
		),
		&output,
	)
	if err != nil {
		//  Handle error
	}

	sort.Sort(sort.Float64Slice(output.results))
	for _, result := range output.results {
		fmt.Println(result)
	}

	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// 7
	// 8
	// 9
	// 10
}
