package tract_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	tract "github.com/23caterpie/Tract"
)

// SquareRootWorkerArg is the key on the tract Request where our square root worker will look for its argument for its work
type SquareRootWorkerArg struct{}

// SquareRootWorkerResult is the key on the tract Request where our square root worker will store the result of its work
type SquareRootWorkerResult struct{}

// These are compiler checks to make sure our implementations satisfy the tract Workers interface
var (
	_ tract.Worker = SquareRootWorker{}
	_ tract.Worker = &SliceArgReaderWorker{}
	_ tract.Worker = &SliceResultsWriterWorker{}
)

// SquareRootWorker is a middle stage worker in a tract.
// Middle stage workers typically will get arguments from the request using:
//     `request.Value(<package-level-struct>).(<expected-type>)`
// then perfrom an operation, and store the results back to the same request using:
//     `context.WithValue(request, <package-level-struct>, <result>)`
// SquareRootWorker performs `math.Sqrt` on its argument.
// SquareRootWorker gets its argument from `SquareRootWorkerArg{}` and stores the result to `SquareRootWorkerResult{}`
type SquareRootWorker struct{}

func (w SquareRootWorker) Work(r tract.Request) (tract.Request, bool) {
	arg, ok := r.Value(SquareRootWorkerArg{}).(float64)
	if !ok {
		return context.WithValue(r, SquareRootWorkerResult{}, math.NaN()), true
	}
	result := math.Sqrt(arg)
	return context.WithValue(r, SquareRootWorkerResult{}, result), true
}

func (w SquareRootWorker) Close() {}

// SliceArgReaderWorker is first stage of the tract.
// First stage workers typically will generate or retrive data from some sort of source,
// such as a queue, file, or user provided data, and store that data on the request using:
//     `context.WithValue(request, <package-level-struct>, <data>)`
// from here, later stages of the tract can use this data. The first stage is also responsible
// for comencing shutdown of the entire tract. When its call to `Work()` returns a false bool,
// it will signal a shutdown of the entire tract. Thus head workers should return false when
// they are done pulling in data from the source.
// SliceArgReaderWorker uses its user populated `arguments` field as a queue. Each time `Work`
// is called it pops an item off of `arguments` and stores it to `SquareRootWorkerArg{}` on the
// tract Request. When there are no more items, false is returned.
// Since SliceArgReaderWorker is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceArgReaderWorker struct {
	arguments []float64
	mutex     sync.Mutex
}

func (w *SliceArgReaderWorker) Work(r tract.Request) (tract.Request, bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if len(w.arguments) == 0 {
		return r, false
	}
	var arg float64
	arg, w.arguments = w.arguments[0], w.arguments[1:]
	return context.WithValue(r, SquareRootWorkerArg{}, arg), true
}

func (w *SliceArgReaderWorker) Close() {}

// SliceResultsWriterWorker is the last stage of the tract.
// Last stage workers typically perform the final operations on a Request. Once a request has passed
// though the final worker, the Request will go thorugh cleanup, send its tract latency metric, then
// it is gone. During this stage all data on the request can be retrieved the same way as the middle
// stages using:
//      `request.Value(<package-level-struct>).(<expected-type>)`
// SliceResultsWriterWorker gets the data from `SquareRootWorkerResult{}` and pushes it onto a list of
// results.
// Since SliceResultsWriterWorker is being used as its own factory, calls to `Work()` must be thread
// safe, thus a mutex is being used.
type SliceResultsWriterWorker struct {
	results []float64
	mutex   sync.Mutex
}

func (w *SliceResultsWriterWorker) Work(r tract.Request) (tract.Request, bool) {
	result, ok := r.Value(SquareRootWorkerResult{}).(float64)
	if !ok {
		return r, false
	}
	w.mutex.Lock()
	w.results = append(w.results, result)
	w.mutex.Unlock()
	return r, true
}

func (w *SliceResultsWriterWorker) Close() {}

// Perpare a few numbers to be square rooted and do it using a tract!
func ExampleTract_serialGroupTract() {
	var resultsWorker SliceResultsWriterWorker
	wholeTract := tract.NewSerialGroupTract("my tract",
		tract.NewWorkerTract("argment reader", 1,
			tract.NewFactoryFromWorker(&SliceArgReaderWorker{
				arguments: []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100},
			}),
		),
		tract.NewWorkerTract("square root", 4, tract.NewFactoryFromWorker(SquareRootWorker{})),
		tract.NewWorkerTract("result reader", 1, tract.NewFactoryFromWorker(&resultsWorker)),
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
