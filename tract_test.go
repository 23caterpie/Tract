package tract_test

import (
	"sync/atomic"
	"testing"

	tract "github.com/23caterpie/Tract"
)

var _ tract.WorkerFactory[int64] = testWorkerFactory[int64]{}

type testWorkerFactory[T any] struct {
	flagMakeWorker func()
	flagClose      func()
	tract.Worker[T]
}

func (f testWorkerFactory[T]) MakeWorker() (tract.Worker[T], error) {
	f.flagMakeWorker()
	return f.Worker, nil
}

func (f testWorkerFactory[_]) Close() { f.flagClose() }

var _ tract.Worker[int64] = testWorker[int64]{}

type testWorker[T any] struct {
	flagClose func()
	work      func(r *tract.Request[T]) (*tract.Request[T], bool)
}

func (w testWorker[T]) Work(r *tract.Request[T]) (*tract.Request[T], bool) {
	return w.work(r)
}

func (w testWorker[_]) Close() { w.flagClose() }

func TestWorkerTract(t *testing.T) {
	type myRequestType struct{}

	// 10 requests
	workSource := []struct{}{9: {}}
	numberOfRequestsProcessed := 0
	numberOfMadeWorkers := 0
	numberOfFactoriesClosed := 0
	numberOfWorkersClosed := 0
	workerTract := tract.NewWorkerTract[myRequestType]("myWorkerTract", 1, testWorkerFactory[myRequestType]{
		flagMakeWorker: func() { numberOfMadeWorkers++ },
		flagClose:      func() { numberOfFactoriesClosed++ },
		Worker: testWorker[myRequestType]{
			flagClose: func() { numberOfWorkersClosed++ },
			work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
				if len(workSource) == 0 {
					return r, false
				}
				workSource = workSource[1:]
				numberOfRequestsProcessed++
				return r, true
			},
		},
	}, tract.WithFactoryClosure[myRequestType](true))

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

	err := workerTract.Init()
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

	wait := workerTract.Start()
	wait()

	// Finished Checks
	expectedNumberOfRequestsProcessed = 10
	expectedNumberOfMadeWorkers = 1
	expectedNumberOfFactoriesClosed = 1
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
	type myRequestType struct{}

	// 10 requests
	workSource := []struct{}{9: {}}
	var (
		numberOfRequestsProcessed int64
		numberOfMadeWorkers       int64
		numberOfFactoriesClosed   int64
		numberOfWorkersClosed     int64
	)
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract[myRequestType]("head", 1, testWorkerFactory[myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
					if len(workSource) == 0 {
						return r, false
					}
					workSource = workSource[1:]
					return r, true
				},
			},
		}, tract.WithFactoryClosure[myRequestType](true)),
		tract.NewWorkerTract[myRequestType]("tail", 2, testWorkerFactory[myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
					atomic.AddInt64(&numberOfRequestsProcessed, 1)
					return r, true
				},
			},
		}, tract.WithFactoryClosure[myRequestType](true)),
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

	err := myTract.Init()
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

	myTract.Start()()

	// Finished Checks
	expectedNumberOfRequestsProcessed = 10
	expectedNumberOfMadeWorkers = 3
	expectedNumberOfFactoriesClosed = 2
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
	type myRequestType struct{}

	// 100 requests
	workSource := []struct{}{99: {}}
	var (
		numberOfParalellRequestsProcessed = [3]int64{}
		numberOfTailRequestsProcessed     int64
		numberOfMadeWorkers               int64
		numberOfFactoriesClosed           int64
		numberOfWorkersClosed             int64
	)
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract[myRequestType]("head", 1, testWorkerFactory[myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
					if len(workSource) == 0 {
						return r, false
					}
					workSource = workSource[1:]
					return r, true
				},
			},
		}, tract.WithFactoryClosure[myRequestType](true)),
		tract.NewParalellGroupTract("myParalellGroupTract",
			tract.NewWorkerTract[myRequestType]("middle1", 1, testWorkerFactory[myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
						atomic.AddInt64(&numberOfParalellRequestsProcessed[0], 1)
						return r, true
					},
				},
			}, tract.WithFactoryClosure[myRequestType](true)),
			tract.NewWorkerTract[myRequestType]("middle2", 2, testWorkerFactory[myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
						atomic.AddInt64(&numberOfParalellRequestsProcessed[1], 1)
						return r, true
					},
				},
			}),
			tract.NewWorkerTract[myRequestType]("middle3", 4, testWorkerFactory[myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
						atomic.AddInt64(&numberOfParalellRequestsProcessed[2], 1)
						return r, true
					},
				},
			}, tract.WithFactoryClosure[myRequestType](true)),
		),
		tract.NewWorkerTract[myRequestType]("tail", 8, testWorkerFactory[myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
					atomic.AddInt64(&numberOfTailRequestsProcessed, 1)
					return r, true
				},
			},
		}, tract.WithFactoryClosure[myRequestType](true)),
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

	err := myTract.Init()
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

	expectedName := "mySerialGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTract.Start()()

	// Finished Checks
	expectedNumberOfParalellRequestsProcessed = 100
	expectedNumberOfTailRequestsProcessed = 100
	expectedNumberOfMadeWorkers = 16
	expectedNumberOfFactoriesClosed = 4
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
	type myRequestType struct{}

	// 100 requests
	workSource := []struct{}{99: {}}
	var (
		numberOfFanOutRequestsProcessed = [3]int64{}
		numberOfTailRequestsProcessed   int64
		numberOfMadeWorkers             int64
		numberOfFactoriesClosed         int64
		numberOfWorkersClosed           int64
	)
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract[myRequestType]("head", 1, testWorkerFactory[myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
					if len(workSource) == 0 {
						return r, false
					}
					workSource = workSource[1:]
					return r, true
				},
			},
		}, tract.WithFactoryClosure[myRequestType](true)),
		tract.NewFanOutGroupTract("myFanOutGroupTract",
			tract.NewWorkerTract[myRequestType]("middle1", 1, testWorkerFactory[myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
						atomic.AddInt64(&numberOfFanOutRequestsProcessed[0], 1)
						return r, true
					},
				},
			}, tract.WithFactoryClosure[myRequestType](true)),
			tract.NewWorkerTract[myRequestType]("middle2", 2, testWorkerFactory[myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
						atomic.AddInt64(&numberOfFanOutRequestsProcessed[1], 1)
						return r, true
					},
				},
			}),
			tract.NewWorkerTract[myRequestType]("middle3", 4, testWorkerFactory[myRequestType]{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker[myRequestType]{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
						atomic.AddInt64(&numberOfFanOutRequestsProcessed[2], 1)
						return r, true
					},
				},
			}, tract.WithFactoryClosure[myRequestType](true)),
		),
		tract.NewWorkerTract[myRequestType]("tail", 8, testWorkerFactory[myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker[myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r *tract.Request[myRequestType]) (*tract.Request[myRequestType], bool) {
					atomic.AddInt64(&numberOfTailRequestsProcessed, 1)
					return r, true
				},
			},
		}, tract.WithFactoryClosure[myRequestType](true)),
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

	err := myTract.Init()
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

	myTract.Start()()

	// Finished Checks
	expectedNumberOfFanOutRequestsProcessed = 300
	expectedNumberOfTailRequestsProcessed = 300
	expectedNumberOfMadeWorkers = 16
	expectedNumberOfFactoriesClosed = 4
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
