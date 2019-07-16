package tract_test

import (
	"context"
	"fmt"

	"git.dev.kochava.com/ccurrin/tract"
)

func ExampleWorkerFactory_tractWorkerFactory() {
	squareRootTract := tract.NewWorkerTract("square root", 4, tract.NewFactoryFromWorker(SquareRootWorker{}))

	factory := tract.NewTractWorkerFactory(squareRootTract)

	worker, err := factory.MakeWorker()
	if err != nil {
		//  Handle error
	}

	args := []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100}

	for _, arg := range args {
		resultReq, success := worker.Work(context.WithValue(context.Background(), SquareRootWorkerArg{}, arg))
		if !success {
			fmt.Println("not successful")
		}
		result := resultReq.Value(SquareRootWorkerResult{}).(float64)
		fmt.Println(result)
	}

	worker.Close()
	factory.Close()

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
