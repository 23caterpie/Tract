package tract_test

import (
	"fmt"
	"sort"

	tract "github.com/23caterpie/Tract"
)

// Perpare a few numbers to be square rooted and do it using a tract!
func ExampleTract_workerTract() {
	var (
		input  = &SliceArgReaderInput{arguments: []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100}}
		output SliceResultsWriterOutput
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
