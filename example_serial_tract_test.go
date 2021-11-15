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
	_ tract.Worker[float64] = SquareRootWorker{}
	_ tract.Worker[float64] = &SliceArgReaderWorker{}
	_ tract.Worker[float64] = &SliceResultsWriterWorker{}
)

// SquareRootWorker is a middle stage worker in a tract.
// Middle stage workers typically will get arguments from the request's Data field
// then perfrom an operation, and store the results back to the same request's Data field.
// SquareRootWorker performs `math.Sqrt` on its argument.
type SquareRootWorker struct{}

func (w SquareRootWorker) Work(r *tract.Request[float64]) (*tract.Request[float64], bool) {
	r.Data = math.Sqrt(r.Data)
	return r, true
}

func (w SquareRootWorker) Close() {}

// SliceArgReaderWorker is first stage of the tract.
// First stage workers typically will generate or retrive data from some sort of source,
// such as a queue, file, or user provided data, and store that data on the request's Data field.
// From here later stages of the tract can use this data. The first stage is also responsible
// for comencing shutdown of the entire tract. When its call to `Work()` returns a false bool,
// it will signal a shutdown of the entire tract. Thus head workers should return false when
// they are done pulling in data from the source.
// SliceArgReaderWorker uses its user populated `arguments` field as a queue. Each time `Work`
// is called it pops an item off of `arguments` and passed it on the tract Request.
// When there are no more items, false is returned.
// Since SliceArgReaderWorker is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceArgReaderWorker struct {
	arguments []float64
	mutex     sync.Mutex
}

func (w *SliceArgReaderWorker) Work(r *tract.Request[float64]) (*tract.Request[float64], bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if len(w.arguments) == 0 {
		return r, false
	}
	r.Data, w.arguments = w.arguments[0], w.arguments[1:]
	return r, true
}

func (w *SliceArgReaderWorker) Close() {}

// SliceResultsWriterWorker is the last stage of the tract.
// Last stage workers typically perform the final operations on a Request
// SliceResultsWriterWorker gets the data from the request and pushes it onto a list of results.
// Since SliceResultsWriterWorker is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceResultsWriterWorker struct {
	results []float64
	mutex   sync.Mutex
}

func (w *SliceResultsWriterWorker) Work(r *tract.Request[float64]) (*tract.Request[float64], bool) {
	w.mutex.Lock()
	w.results = append(w.results, r.Data)
	w.mutex.Unlock()
	return r, true
}

func (w *SliceResultsWriterWorker) Close() {}

// Perpare a few numbers to be square rooted and do it using a tract!
func ExampleTract_serialGroupTract() {
	var resultsWorker SliceResultsWriterWorker
	wholeTract := tract.NewSerialGroupTract("my tract",
		tract.NewWorkerTract("argment reader", 1,
			tract.NewFactoryFromWorker[float64](&SliceArgReaderWorker{
				arguments: []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100},
			}),
		),
		tract.NewWorkerTract("square root", 4, tract.NewFactoryFromWorker[float64](SquareRootWorker{})),
		tract.NewWorkerTract("result reader", 1, tract.NewFactoryFromWorker[float64](&resultsWorker)),
	)

	err := wholeTract.Init()
	if err != nil {
		//  Handle error
	}

	wait := wholeTract.Start()
	wait()

	sort.Sort(sort.Float64Slice(resultsWorker.results))
	for _, result := range resultsWorker.results {
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
