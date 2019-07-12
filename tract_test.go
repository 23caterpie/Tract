package tract_test

import (
	"sync/atomic"
	"testing"

	"git.dev.kochava.com/ccurrin/tract"
)

var _ tract.WorkerFactory = testWorkerFactory{}

type testWorkerFactory struct {
	flagMakeWorker func()
	flagClose      func()
	tract.Worker
}

func (f testWorkerFactory) MakeWorker() (tract.Worker, error) {
	f.flagMakeWorker()
	return f.Worker, nil
}

func (f testWorkerFactory) Close() { f.flagClose() }

var _ tract.Worker = testWorker{}

type testWorker struct {
	flagClose func()
	work      func(r tract.Request) (tract.Request, bool)
}

func (w testWorker) Work(r tract.Request) (tract.Request, bool) {
	return w.work(r)
}

func (w testWorker) Close() { w.flagClose() }

func TestWorkerTract(t *testing.T) {
	// 10 requests
	workSource := []struct{}{9: {}}
	numberOfRequestsProcessed := 0
	numberOfMadeWorkers := 0
	numberOfFactoriesClosed := 0
	numberOfWorkersClosed := 0
	workerTract := tract.NewWorkerTract("myWorkerTract", 1, testWorkerFactory{
		flagMakeWorker: func() { numberOfMadeWorkers++ },
		flagClose:      func() { numberOfFactoriesClosed++ },
		Worker: testWorker{
			flagClose: func() { numberOfWorkersClosed++ },
			work: func(r tract.Request) (tract.Request, bool) {
				if len(workSource) == 0 {
					return r, false
				}
				workSource = workSource[1:]
				numberOfRequestsProcessed++
				return r, true
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
	// 10 requests
	workSource := []struct{}{9: {}}
	var (
		numberOfRequestsProcessed int64
		numberOfMadeWorkers       int64
		numberOfFactoriesClosed   int64
		numberOfWorkersClosed     int64
	)
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract("head", 1, testWorkerFactory{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r tract.Request) (tract.Request, bool) {
					if len(workSource) == 0 {
						return r, false
					}
					workSource = workSource[1:]
					return r, true
				},
			},
		}),
		tract.NewWorkerTract("tail", 2, testWorkerFactory{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r tract.Request) (tract.Request, bool) {
					atomic.AddInt64(&numberOfRequestsProcessed, 1)
					return r, true
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
	// 100 requests
	workSource := []struct{}{99: {}}
	var (
		numberOfRequestsProcessed = [3]int64{}
		numberOfMadeWorkers       int64
		numberOfFactoriesClosed   int64
		numberOfWorkersClosed     int64
	)
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract("head", 1, testWorkerFactory{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
			Worker: testWorker{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(r tract.Request) (tract.Request, bool) {
					if len(workSource) == 0 {
						return r, false
					}
					workSource = workSource[1:]
					return r, true
				},
			},
		}),
		tract.NewParalellGroupTract("myParalellGroupTract",
			tract.NewWorkerTract("tail", 1, testWorkerFactory{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[0], 1)
						return r, true
					},
				},
			}),
			tract.NewWorkerTract("tail", 2, testWorkerFactory{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[1], 1)
						return r, true
					},
				},
			}),
			tract.NewWorkerTract("tail", 4, testWorkerFactory{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[2], 1)
						return r, true
					},
				},
			}),
		),
	)

	// Pre-Init Checks
	var (
		expectedNumberOfRequestsProcessed int64 = 0
		expectedNumberOfMadeWorkers       int64 = 0
		expectedNumberOfFactoriesClosed   int64 = 0
		expectedNumberOfWorkersClosed     int64 = 0
	)
	var totalNumberOfRequestsProcessed int64
	for _, n := range numberOfRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	if totalNumberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, totalNumberOfRequestsProcessed)
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
	expectedNumberOfMadeWorkers = 8
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0

	totalNumberOfRequestsProcessed = 0
	for _, n := range numberOfRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	if totalNumberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, totalNumberOfRequestsProcessed)
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
	expectedNumberOfRequestsProcessed = 100
	expectedNumberOfMadeWorkers = 8
	expectedNumberOfFactoriesClosed = 4
	expectedNumberOfWorkersClosed = 8

	totalNumberOfRequestsProcessed = 0
	for _, n := range numberOfRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	if totalNumberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, totalNumberOfRequestsProcessed)
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

func TestFanOutlGroupTract(t *testing.T) {
	// 100 requests
	workSource := []struct{}{99: {}}
	numberOfRequestsProcessed := [3]int64{}
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerTract("head", 1, testWorkerFactory{
			flagMakeWorker: func() {},
			flagClose:      func() {},
			Worker: testWorker{
				flagClose: func() {},
				work: func(r tract.Request) (tract.Request, bool) {
					if len(workSource) == 0 {
						return r, false
					}
					workSource = workSource[1:]
					return r, true
				},
			},
		}),
		tract.NewFanOutGroupTract("myFanOutGroupTract",
			tract.NewWorkerTract("tail", 1, testWorkerFactory{
				flagMakeWorker: func() {},
				flagClose:      func() {},
				Worker: testWorker{
					flagClose: func() {},
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[0], 1)
						return r, true
					},
				},
			}),
			tract.NewWorkerTract("tail", 2, testWorkerFactory{
				flagMakeWorker: func() {},
				flagClose:      func() {},
				Worker: testWorker{
					flagClose: func() {},
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[1], 1)
						return r, true
					},
				},
			}),
			tract.NewWorkerTract("tail", 4, testWorkerFactory{
				flagMakeWorker: func() {},
				flagClose:      func() {},
				Worker: testWorker{
					flagClose: func() {},
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[2], 1)
						return r, true
					},
				},
			}),
		),
	)

	err := myTract.Init()
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	expectedName := "mySerialGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTract.Start()()

	var totalNumberOfRequestsProcessed int64
	for _, n := range numberOfRequestsProcessed {
		totalNumberOfRequestsProcessed += n
	}
	expectedNumberOfRequestsProcessed := int64(300)
	if totalNumberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, totalNumberOfRequestsProcessed)
	}
}
