package tract_test

import (
	"context"
	"reflect"
	"sync"
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
	numberOfCleanups := 0
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
				r = tract.AddRequestCleanup(r, func(tract.Request, bool) { numberOfCleanups++ })
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
		expectedNumberOfCleanups          = 0
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
	if numberOfCleanups != expectedNumberOfCleanups {
		t.Errorf(`number of request cleanups: expected %d, received %d`, expectedNumberOfCleanups, numberOfCleanups)
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
	expectedNumberOfCleanups = 0
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
	if numberOfCleanups != expectedNumberOfCleanups {
		t.Errorf(`number of request cleanups: expected %d, received %d`, expectedNumberOfCleanups, numberOfCleanups)
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
	expectedNumberOfCleanups = 10
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
	if numberOfCleanups != expectedNumberOfCleanups {
		t.Errorf(`number of request cleanups: expected %d, received %d`, expectedNumberOfCleanups, numberOfCleanups)
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

func TestFanOutGroupTract(t *testing.T) {
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
		tract.NewFanOutGroupTract("myFanOutGroupTract",
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
	expectedNumberOfRequestsProcessed = 300
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

func TestTractWorker(t *testing.T) {
	type testLabel struct{}
	var (
		numberOfRequestsProcessed    = [2]int64{}
		numberOfMadeWorkers          int64
		numberOfFactoriesClosed      int64
		numberOfWorkersClosed        int64
		numberOfRequestCleanups      = [3]map[int]struct{}{{}, {}, {}}
		numberOfRequestCleanupsMutex = sync.Mutex{}
	)
	myWorkerFactory := tract.NewTractWorkerFactory(
		tract.NewSerialGroupTract("tractWorkerGroup",
			tract.NewWorkerTract("tractWorker1", 4, testWorkerFactory{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[0], 1)
						r = tract.AddRequestCleanup(r, func(req tract.Request, success bool) {
							if success {
								tractKey := 0
								key, _ := req.Value(testLabel{}).(int)
								numberOfRequestCleanupsMutex.Lock()
								if _, found := numberOfRequestCleanups[tractKey][key]; found {
									t.Errorf("request %d has already been cleaned up for tract %d", key, tractKey)
								}
								numberOfRequestCleanups[tractKey][key] = struct{}{}
								numberOfRequestCleanupsMutex.Unlock()
							}
						})
						return r, true
					},
				},
			}),
			tract.NewWorkerTract("tractWorker2", 1, testWorkerFactory{
				flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
				flagClose:      func() { atomic.AddInt64(&numberOfFactoriesClosed, 1) },
				Worker: testWorker{
					flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
					work: func(r tract.Request) (tract.Request, bool) {
						atomic.AddInt64(&numberOfRequestsProcessed[1], 1)
						r = tract.AddRequestCleanup(r, func(req tract.Request, success bool) {
							if success {
								tractKey := 1
								key, _ := req.Value(testLabel{}).(int)
								numberOfRequestCleanupsMutex.Lock()
								if _, found := numberOfRequestCleanups[tractKey][key]; found {
									t.Errorf("request %d has already been cleaned up for tract %d", key, tractKey)
								}
								numberOfRequestCleanups[tractKey][key] = struct{}{}
								numberOfRequestCleanupsMutex.Unlock()
							}
						})
						return r, true
					},
				},
			}),
		),
	)

	// Pre-MakeWorker Checks
	var (
		expectedNumberOfRequestsProcessed       = [2]int64{0, 0}
		expectedNumberOfMadeWorkers       int64 = 0
		expectedNumberOfFactoriesClosed   int64 = 0
		expectedNumberOfWorkersClosed     int64 = 0
		expectedNumberOfRequestCleanups         = [3]map[int]struct{}{{}, {}, {}}
	)
	if !reflect.DeepEqual(numberOfRequestsProcessed, expectedNumberOfRequestsProcessed) {
		t.Errorf(`number of requests processed: expected %v, received %v`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
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
	if !reflect.DeepEqual(numberOfRequestCleanups, expectedNumberOfRequestCleanups) {
		t.Errorf(`number of request clean ups run: expected %v, received %v`, expectedNumberOfRequestCleanups, numberOfRequestCleanups)
	}

	myWorker, err := myWorkerFactory.MakeWorker()
	if err != nil {
		t.Errorf("unexpected error during tract worker creation %v", err)
	}

	// Pre-Work Checks
	expectedNumberOfRequestsProcessed = [2]int64{0, 0}
	expectedNumberOfMadeWorkers = 5
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0
	expectedNumberOfRequestCleanups = [3]map[int]struct{}{{}, {}, {}}

	if !reflect.DeepEqual(numberOfRequestsProcessed, expectedNumberOfRequestsProcessed) {
		t.Errorf(`number of requests processed: expected %v, received %v`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
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
	if !reflect.DeepEqual(numberOfRequestCleanups, expectedNumberOfRequestCleanups) {
		t.Errorf(`number of request clean ups run: expected %v, received %v`, expectedNumberOfRequestCleanups, numberOfRequestCleanups)
	}

	myResults := make([]tract.Request, 100)
	wg := sync.WaitGroup{}
	for i := range myResults {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			var success bool
			myRequest := context.WithValue(context.Background(), testLabel{}, j)
			myRequest = tract.AddRequestCleanup(myRequest, func(req tract.Request, success bool) {
				if success {
					tractKey := 2
					key, _ := req.Value(testLabel{}).(int)
					numberOfRequestCleanupsMutex.Lock()
					if _, found := numberOfRequestCleanups[tractKey][key]; found {
						t.Errorf("request %d has already been cleaned up for tract %d", key, tractKey)
					}
					numberOfRequestCleanups[tractKey][key] = struct{}{}
					numberOfRequestCleanupsMutex.Unlock()
				}
			})
			myResults[j], success = myWorker.Work(myRequest)
			if !success {
				t.Errorf("call to work did not succeed")
			}
		}(i)
	}
	wg.Wait()
	for i := range myResults {
		label := myResults[i].Value(testLabel{})
		if label != i {
			t.Errorf("expected same request back from function call, not a different one: expected %d, received %+#v", i, label)
		}
	}

	// Pre-Close Checks
	expectedNumberOfRequestsProcessed = [2]int64{100, 100}
	expectedNumberOfMadeWorkers = 5
	expectedNumberOfFactoriesClosed = 0
	expectedNumberOfWorkersClosed = 0
	expectedNumberOfRequestCleanups = [3]map[int]struct{}{
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
		{},
	}

	if !reflect.DeepEqual(numberOfRequestsProcessed, expectedNumberOfRequestsProcessed) {
		t.Errorf(`number of requests processed: expected %v, received %v`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
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
	if !reflect.DeepEqual(numberOfRequestCleanups, expectedNumberOfRequestCleanups) {
		t.Errorf(`number of request clean ups run: expected %v, received %v`, expectedNumberOfRequestCleanups, numberOfRequestCleanups)
	}

	myWorker.Close()
	myWorkerFactory.Close()

	// Pre-Final Cleanup Checks
	expectedNumberOfRequestsProcessed = [2]int64{100, 100}
	expectedNumberOfMadeWorkers = 5
	expectedNumberOfFactoriesClosed = 2
	expectedNumberOfWorkersClosed = 5
	expectedNumberOfRequestCleanups = [3]map[int]struct{}{
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
		{},
	}

	if !reflect.DeepEqual(numberOfRequestsProcessed, expectedNumberOfRequestsProcessed) {
		t.Errorf(`number of requests processed: expected %v, received %v`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
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
	if !reflect.DeepEqual(numberOfRequestCleanups, expectedNumberOfRequestCleanups) {
		t.Errorf(`number of request clean ups run: expected %v, received %v`, expectedNumberOfRequestCleanups, numberOfRequestCleanups)
	}

	// Cleanups we set before sending the request through the tract worker should not have been called, and should still exist for us to call.
	// Any cleanups the tract inside the worker performed should have been called already and removed.
	for i := range myResults {
		tract.CleanupRequest(myResults[i], true)
	}

	// Final Checks
	expectedNumberOfRequestsProcessed = [2]int64{100, 100}
	expectedNumberOfMadeWorkers = 5
	expectedNumberOfFactoriesClosed = 2
	expectedNumberOfWorkersClosed = 5
	expectedNumberOfRequestCleanups = [3]map[int]struct{}{
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
		{
			0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			10: {}, 11: {}, 12: {}, 13: {}, 14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {},
			20: {}, 21: {}, 22: {}, 23: {}, 24: {}, 25: {}, 26: {}, 27: {}, 28: {}, 29: {},
			30: {}, 31: {}, 32: {}, 33: {}, 34: {}, 35: {}, 36: {}, 37: {}, 38: {}, 39: {},
			40: {}, 41: {}, 42: {}, 43: {}, 44: {}, 45: {}, 46: {}, 47: {}, 48: {}, 49: {},
			50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 55: {}, 56: {}, 57: {}, 58: {}, 59: {},
			60: {}, 61: {}, 62: {}, 63: {}, 64: {}, 65: {}, 66: {}, 67: {}, 68: {}, 69: {},
			70: {}, 71: {}, 72: {}, 73: {}, 74: {}, 75: {}, 76: {}, 77: {}, 78: {}, 79: {},
			80: {}, 81: {}, 82: {}, 83: {}, 84: {}, 85: {}, 86: {}, 87: {}, 88: {}, 89: {},
			90: {}, 91: {}, 92: {}, 93: {}, 94: {}, 95: {}, 96: {}, 97: {}, 98: {}, 99: {},
		},
	}

	if !reflect.DeepEqual(numberOfRequestsProcessed, expectedNumberOfRequestsProcessed) {
		t.Errorf(`number of requests processed: expected %v, received %v`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
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
	if !reflect.DeepEqual(numberOfRequestCleanups, expectedNumberOfRequestCleanups) {
		t.Errorf(`number of request clean ups run: expected %v, received %v`, expectedNumberOfRequestCleanups, numberOfRequestCleanups)
	}
}
