package tract_test

import (
	"fmt"
	"sort"

	tract "github.com/23caterpie/Tract"
)

func ExampleLinker() {
	var (
		input         = &SliceArgReaderInput{arguments: []float64{0, 1, 256, 6561, 65536, 390625, 1679616, 5764801, 16777216, 43046721, 100000000}}
		workerFactory = tract.NewFactoryFromWorker[float64, float64](SquareRootWorker{})
		output        SliceResultsWriterOutput
	)

	err := tract.Run[float64, float64](
		input,
		tract.NewNamedLinker[float64, float64, float64](
			"group",
			tract.NewWorkerTract("worker tract 1", 1, workerFactory),
		).Link(tract.NewLinker[float64, float64, float64](
			tract.NewWorkerTract("worker tract 2", 1, workerFactory),
		).Link(
			tract.NewWorkerTract("worker tract 3", 1, workerFactory),
		)),
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
