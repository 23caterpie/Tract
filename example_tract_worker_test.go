package tract_test

import (
	"context"
	"fmt"
	"sync"

	tract "github.com/23caterpie/Tract"
)

func ExampleWorkerFactory_tractWorkerFactorySync() {
	squareRootTract := tract.NewWorkerTract("square root", 4, tract.NewFactoryFromWorker(SquareRootWorker{}))

	factory := tract.NewTractWorkerFactory(squareRootTract)

	worker, err := factory.MakeWorker()
	if err != nil {
		//  Handle error
		return
	}

	args := []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100}

	for _, arg := range args {
		resultReq, success := worker.Work(context.WithValue(context.Background(), SquareRootWorkerArg{}, arg))
		if !success {
			fmt.Println("not successful")
		}
		result, _ := resultReq.Value(SquareRootWorkerResult{}).(float64)
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

func ExampleWorkerFactory_tractWorkerFactoryAsync() {
	squareRootTract := tract.NewWorkerTract("square root", 4, tract.NewFactoryFromWorker(SquareRootWorker{}))

	factory := tract.NewTractWorkerFactory(squareRootTract)

	worker, err := factory.MakeWorker()
	if err != nil {
		//  Handle error
		return
	}

	args := []float64{0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	results := make([]float64, len(args))

	wg := sync.WaitGroup{}
	for i := range args {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			resultReq, success := worker.Work(context.WithValue(context.Background(), SquareRootWorkerArg{}, args[j]))
			if !success {
				fmt.Println("not successful")
			}
			results[j], _ = resultReq.Value(SquareRootWorkerResult{}).(float64)
		}(i)
	}
	wg.Wait()
	for _, result := range results {
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
