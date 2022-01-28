package tract_test

import (
	"fmt"
	"io/ioutil"
	"math"
	"sort"

	tract "github.com/23caterpie/Tract"
)

// Perpare a few numbers to be square rooted and do it using a tract!
func ExampleTract_sqrt_workerTract() {
	var (
		input  = NewSliceArgReaderInput([]float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100})
		output SliceResultsWriterOutput[float64]
	)

	err := tract.Run[float64, float64](
		input,
		tract.NewWorkerTract("square root", 4, NewSquareRootWorker()),
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

// Same as above but showing simple cases can just use a func direcly.
func ExampleTract_sqrt_basicWorkerFuncTract() {
	var (
		input  = NewSliceArgReaderInput([]float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100})
		output SliceResultsWriterOutput[float64]
	)

	err := tract.Run[float64, float64](
		input,
		tract.NewBasicWorkerFuncTract("square root", 4, math.Sqrt),
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

func ExampleTract_writer_basicWorkerFuncTract() {
	var (
		input  = NewSliceArgReaderInput([][]byte{[]byte("hello"), []byte("world!")})
		output SliceResultsWriterOutput[int]
	)

	err := tract.Run[[]byte, int](
		input,
		tract.NewErrorWorkerFuncTract("square root", 4, ioutil.Discard.Write),
		&output,
	)
	if err != nil {
		//  Handle error
	}

	sort.Sort(sort.IntSlice(output.results))
	for _, result := range output.results {
		fmt.Println(result)
	}

	// Output:
	// 5
	// 6
}
