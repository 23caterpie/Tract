package tract_test

import (
	"sync/atomic"
	"testing"

	tract "github.com/23caterpie/Tract"
)

var _ tract.WorkerFactory[int64, string] = testWorkerFactory[int64, string]{}

type testWorkerFactory[InputType, OutputType any] struct {
	flagMakeWorker func()
	flagClose      func()
	tract.Worker[InputType, OutputType]
}

func (f testWorkerFactory[InputType, OutputType]) MakeWorker() (tract.Worker[InputType, OutputType], error) {
	f.flagMakeWorker()
	return f.Worker, nil
}

func (f testWorkerFactory[InputType, OutputType]) Close() { f.flagClose() }

var _ tract.Worker[int64, string] = testWorker[int64, string]{}

type testWorker[InputType, OutputType any] struct {
	flagClose func()
	work      func(InputType) (OutputType, bool)
}

func (w testWorker[InputType, OutputType]) Work(i InputType) (OutputType, bool) {
	return w.work(i)
}

func (w testWorker[InputType, OutputType]) Close() { w.flagClose() }

func TestWorkerTract(t *testing.T) {
	type (
		myInputType  struct{}
		myOutputType struct{}
	)

	// 10 requests
	workSource := []struct{}{9: {}}
	numberOfRequestsProcessed := 0
	numberOfMadeWorkers := 0
	numberOfFactoriesClosed := 0
	numberOfWorkersClosed := 0
	workerTract := tract.NewWorkerTract[myInputType, myOutputType]("myWorkerTract", 1, testWorkerFactory[myInputType, myOutputType]{
		flagMakeWorker: func() { numberOfMadeWorkers++ },
		flagClose:      func() { numberOfFactoriesClosed++ },
		Worker: testWorker[myInputType, myOutputType]{
			flagClose: func() { numberOfWorkersClosed++ },
			work: func(_ myInputType) (myOutputType, bool) {
				if len(workSource) == 0 {
					return myOutputType{}, false
				}
				workSource = workSource[1:]
				numberOfRequestsProcessed++
				return myOutputType{}, true
			},
		},
	})

	// Pre-Init Checks
	var (
		expectedNumberOfRequestsProcessed = 0
		expectedNumberOfMadeWorkers       = 0
		expectedNumberOfFactoriesClosed   = 0
		expectedNumberOfWorkersClosed     = 0
	)
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	workerTractStarter, err := workerTract.Init(tract.InputGenerator[myInputType]{}, tract.FinalOutput[myOutputType]{})
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfRequestsProcessed = 0
	expectedNumberOfMadeWorkers = 1
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	expectedName := "myWorkerTract"
	actualName := workerTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	workerTractWaiter := workerTractStarter.Start()
	workerTractWaiter.Wait()

	// Finished Checks
	expectedNumberOfRequestsProcessed = 10
	expectedNumberOfMadeWorkers = 1
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 1
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
}

func TestSerialGroupTract(t *testing.T) {
	type (
		myInputType  struct{}
		myInnerType  struct{}
		myOutputType struct{}
	)

	// 10 requests
	workSource := []struct{}{9: {}}
	var (
		numberOfRequestsProcessed int64
		numberOfMadeWorkers       int64
		numberOfFactoriesClosed   int64
		numberOfWorkersClosed     int64
	)
	// This is terrible.. I wish struct methods could have type parameters...
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract[myInputType, myInnerType]("head", 1, testWorkerFactory[myInputType, myInnerType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myInputType, myInnerType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ myInputType) (myInnerType, bool) {
					if len(workSource) == 0 {
						return myInnerType{}, false
					}
					workSource = workSource[1:]
					return myInnerType{}, true
				},
			},
		}),
		tract.NewWorkerTract[myInnerType, myOutputType]("tail", 2, testWorkerFactory[myInnerType, myOutputType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myInnerType, myOutputType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ myInnerType) (myOutputType, bool) {
					atomic.AddInt64(&numberOfRequestsProcessed, 1)
					return myOutputType{}, true
				},
			},
		}),
	)

	// Pre-Init Checks
	var (
		expectedNumberOfRequestsProcessed int64 = 0
		expectedNumberOfMadeWorkers       int64 = 0
		expectedNumberOfFactoriesClosed   int64 = 0
		expectedNumberOfWorkersClosed     int64 = 0
	)
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	myTractStarter, err := myTract.Init(tract.InputGenerator[myInputType]{}, tract.FinalOutput[myOutputType]{})
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfRequestsProcessed = 0
	expectedNumberOfMadeWorkers = 3
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	expectedName := "mySerialGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTractStarter.Start().Wait()

	// Finished Checks
	expectedNumberOfRequestsProcessed = 10
	expectedNumberOfMadeWorkers = 3
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 3
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
}

func TestParalellGroupTract(t *testing.T) {
	type (
		myRequestType struct{}
	)

	// 100 requests
	workSource := []struct{}{99: {}}
	var (
		numberOfParalellRequestsProcessed = [3]int64{}
		numberOfTailRequestsProcessed     int64
		numberOfMadeWorkers               int64
		numberOfFactoriesClosed           int64
		numberOfWorkersClosed             int64
	)
	myTract := tract.NewSerialGroupTract[myRequestType, myRequestType, myRequestType]("mySerialGroupTract1",
		tract.NewWorkerTract[myRequestType, myRequestType]("head", 1, testWorkerFactory[myRequestType, myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType, myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ myRequestType) (myRequestType, bool) {
					if len(workSource) == 0 {
						return myRequestType{}, false
					}
					workSource = workSource[1:]
					return myRequestType{}, true
				},
			},
		}),
		tract.NewParalellGroupTract[myRequestType, myRequestType]("myParalellGroupTract",
			tract.NewWorkerTract[myRequestType, myRequestType]("middle1", 1, testWorkerFactory[myRequestType, myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType, myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(_ myRequestType) (myRequestType, bool) {
						atomic.AddInt64(&numberOfParalellRequestsProcessed[0], 1)
						return myRequestType{}, true
					},
				},
			}),
			tract.NewWorkerTract[myRequestType, myRequestType]("middle2", 2, testWorkerFactory[myRequestType, myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType, myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(_ myRequestType) (myRequestType, bool) {
						atomic.AddInt64(&numberOfParalellRequestsProcessed[1], 1)
						return myRequestType{}, true
					},
				},
			}),
			tract.NewWorkerTract[myRequestType, myRequestType]("middle3", 4, testWorkerFactory[myRequestType, myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType, myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(_ myRequestType) (myRequestType, bool) {
						atomic.AddInt64(&numberOfParalellRequestsProcessed[2], 1)
						return myRequestType{}, true
					},
				},
			}),
		),
	)
	myTract = tract.NewSerialGroupTract[myRequestType, myRequestType, myRequestType]("mySerialGroupTract2", myTract,
		tract.NewWorkerTract[myRequestType, myRequestType]("tail", 8, testWorkerFactory[myRequestType, myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType, myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ myRequestType) (myRequestType, bool) {
					atomic.AddInt64(&numberOfTailRequestsProcessed, 1)
					return myRequestType{}, true
				},
			},
		}),
	)

	// Pre-Init Checks
	var (
		expectedNumberOfParalellRequestsProcessed int64 = 0
		expectedNumberOfTailRequestsProcessed     int64 = 0
		expectedNumberOfMadeWorkers               int64 = 0
		expectedNumberOfFactoriesClosed           int64 = 0
		expectedNumberOfWorkersClosed             int64 = 0
	)
	var totalNumberOfParalellRequestsProcessed int64
	for _, n := range numberOfParalellRequestsProcessed {
		totalNumberOfParalellRequestsProcessed += n
	}
	if totalNumberOfParalellRequestsProcessed != expectedNumberOfParalellRequestsProcessed {
		t.Errorf(`number of paralell requests processed: expected %d, received %d`, expectedNumberOfParalellRequestsProcessed, totalNumberOfParalellRequestsProcessed)
	}
	if numberOfTailRequestsProcessed != expectedNumberOfTailRequestsProcessed {
		t.Errorf(`number of tail requests processed: expected %d, received %d`, expectedNumberOfTailRequestsProcessed, numberOfTailRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	myTractStarter, err := myTract.Init(tract.InputGenerator[myRequestType]{}, tract.FinalOutput[myRequestType]{})
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfParalellRequestsProcessed = 0
	expectedNumberOfTailRequestsProcessed = 0
	expectedNumberOfMadeWorkers = 16
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0

	totalNumberOfParalellRequestsProcessed = 0
	for _, n := range numberOfParalellRequestsProcessed {
		totalNumberOfParalellRequestsProcessed += n
	}
	if totalNumberOfParalellRequestsProcessed != expectedNumberOfParalellRequestsProcessed {
		t.Errorf(`number of paralell requests processed: expected %d, received %d`, expectedNumberOfParalellRequestsProcessed, totalNumberOfParalellRequestsProcessed)
	}
	if numberOfTailRequestsProcessed != expectedNumberOfTailRequestsProcessed {
		t.Errorf(`number of tail requests processed: expected %d, received %d`, expectedNumberOfTailRequestsProcessed, numberOfTailRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	expectedName := "mySerialGroupTract2"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTractStarter.Start().Wait()

	// Finished Checks
	expectedNumberOfParalellRequestsProcessed = 100
	expectedNumberOfTailRequestsProcessed = 100
	expectedNumberOfMadeWorkers = 16
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 16

	totalNumberOfParalellRequestsProcessed = 0
	for _, n := range numberOfParalellRequestsProcessed {
		totalNumberOfParalellRequestsProcessed += n
	}
	if totalNumberOfParalellRequestsProcessed != expectedNumberOfParalellRequestsProcessed {
		t.Errorf(`number of paralell requests processed: expected %d, received %d`, expectedNumberOfParalellRequestsProcessed, totalNumberOfParalellRequestsProcessed)
	}
	if numberOfTailRequestsProcessed != expectedNumberOfTailRequestsProcessed {
		t.Errorf(`number of tail requests processed: expected %d, received %d`, expectedNumberOfTailRequestsProcessed, numberOfTailRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
}

func TestFanOutGroupTract(t *testing.T) {
	type (
		myInputType struct{}
		myInnerType struct{}
		myOutputType struct{}
	)

	// 100 requests
	workSource := []struct{}{99: {}}
	var (
		numberOfFanOutRequestsProcessed = [3]int64{}
		numberOfTailRequestsProcessed   int64
		numberOfMadeWorkers             int64
		numberOfFactoriesClosed         int64
		numberOfWorkersClosed           int64
	)
	myTract := tract.NewSerialGroupTract[myInputType, myOutputType, myOutputType]("mySerialGroupTract",
		tract.NewFanOutGroupTract[myInputType, myInnerType, myOutputType]("myFanOutGroupTract",
			tract.NewWorkerTract[myInputType, myInnerType]("head", 1, testWorkerFactory[myInputType, myInnerType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myInputType, myInnerType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r myInputType) (myInnerType, bool) {
						if len(workSource) == 0 {
							return myInnerType{}, false
						}
						workSource = workSource[1:]
						return myInnerType{}, true
					},
				},
			}),
			tract.NewWorkerTract[myInnerType, myOutputType]("middle1", 1, testWorkerFactory[myInnerType, myOutputType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myInnerType, myOutputType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r myInnerType) (myOutputType, bool) {
						atomic.AddInt64(&numberOfFanOutRequestsProcessed[0], 1)
						return myOutputType{}, true
					},
				},
			}),
			tract.NewWorkerTract[myInnerType, myOutputType]("middle2", 2, testWorkerFactory[myInnerType, myOutputType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myInnerType, myOutputType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r myInnerType) (myOutputType, bool) {
						atomic.AddInt64(&numberOfFanOutRequestsProcessed[1], 1)
						return myOutputType{}, true
					},
				},
			}),
			tract.NewWorkerTract[myInnerType, myOutputType]("middle3", 4, testWorkerFactory[myInnerType, myOutputType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myInnerType, myOutputType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r myInnerType) (myOutputType, bool) {
						atomic.AddInt64(&numberOfFanOutRequestsProcessed[2], 1)
						return myOutputType{}, true
					},
				},
			}),
		),
		tract.NewWorkerTract[myOutputType, myOutputType]("tail", 8, testWorkerFactory[myOutputType, myOutputType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myOutputType, myOutputType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r myOutputType) (myOutputType, bool) {
					atomic.AddInt64(&numberOfTailRequestsProcessed, 1)
					return myOutputType{}, true
				},
			},
		}),
	)

	// Pre-Init Checks
	var (
		expectedNumberOfFanOutRequestsProcessed int64 = 0
		expectedNumberOfTailRequestsProcessed   int64 = 0
		expectedNumberOfMadeWorkers             int64 = 0
		expectedNumberOfFactoriesClosed         int64 = 0
		expectedNumberOfWorkersClosed           int64 = 0
	)
	var totalNumberOfRequestsProcessed int64
	for _, n := range numberOfFanOutRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	if totalNumberOfRequestsProcessed != expectedNumberOfFanOutRequestsProcessed {
		t.Errorf(`number of fan out requests processed: expected %d, received %d`, expectedNumberOfFanOutRequestsProcessed, totalNumberOfRequestsProcessed)
	}
	if numberOfTailRequestsProcessed != expectedNumberOfTailRequestsProcessed {
		t.Errorf(`number of tail requests processed: expected %d, received %d`, expectedNumberOfTailRequestsProcessed, numberOfTailRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	myTractStarter, err := myTract.Init(tract.InputGenerator[myInputType]{}, tract.FinalOutput[myOutputType]{})
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfFanOutRequestsProcessed = 0
	expectedNumberOfTailRequestsProcessed = 0
	expectedNumberOfMadeWorkers = 16
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0

	totalNumberOfRequestsProcessed = 0
	for _, n := range numberOfFanOutRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	if totalNumberOfRequestsProcessed != expectedNumberOfFanOutRequestsProcessed {
		t.Errorf(`number of fan out requests processed: expected %d, received %d`, expectedNumberOfFanOutRequestsProcessed, totalNumberOfRequestsProcessed)
	}
	if numberOfTailRequestsProcessed != expectedNumberOfTailRequestsProcessed {
		t.Errorf(`number of tail requests processed: expected %d, received %d`, expectedNumberOfTailRequestsProcessed, numberOfTailRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}

	expectedName := "mySerialGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTractStarter.Start().Wait()

	// Finished Checks
	expectedNumberOfFanOutRequestsProcessed = 300
	expectedNumberOfTailRequestsProcessed = 300
	expectedNumberOfMadeWorkers = 16
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 16

	totalNumberOfRequestsProcessed = 0
	for _, n := range numberOfFanOutRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	if totalNumberOfRequestsProcessed != expectedNumberOfFanOutRequestsProcessed {
		t.Errorf(`number of fan out requests processed: expected %d, received %d`, expectedNumberOfFanOutRequestsProcessed, totalNumberOfRequestsProcessed)
	}
	if numberOfTailRequestsProcessed != expectedNumberOfTailRequestsProcessed {
		t.Errorf(`number of tail requests processed: expected %d, received %d`, expectedNumberOfTailRequestsProcessed, numberOfTailRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfFactoriesClosed != expectedNumberOfFactoriesClosed {
		t.Errorf(`number of factory closures: expected %d, received %d`, expectedNumberOfFactoriesClosed, numberOfFactoriesClosed)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
}
