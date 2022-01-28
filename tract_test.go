package tract_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	tract "github.com/23caterpie/Tract"
)

var _ tract.WorkerFactory[int64, string, testWorker[int64, string]] = testWorkerFactory[int64, string]{}

type testWorkerFactory[InputType, OutputType tract.Request] struct {
	flagMakeWorker func()
	testWorker[InputType, OutputType]
}

func (f testWorkerFactory[InputType, OutputType]) MakeWorker() (testWorker[InputType, OutputType], error) {
	f.flagMakeWorker()
	return f.testWorker, nil
}

var _ tract.Worker[int64, string] = testWorker[int64, string]{}

type testWorker[InputType, OutputType tract.Request] struct {
	flagClose func()
	work      func(context.Context, InputType) (OutputType, bool)
}

func (w testWorker[InputType, OutputType]) Work(ctx context.Context, i InputType) (OutputType, bool) {
	return w.work(ctx, i)
}

func (w testWorker[InputType, OutputType]) Close() { w.flagClose() }

var _ tract.Input[int64] = testInput[int64]{}

type testInput[T tract.Request] struct {
	flagGet func()
	get     func() (T, bool)
}

func (i testInput[T]) Get() (T, bool) {
	i.flagGet()
	return i.get()
}

func newSourceTestInput[T tract.Request](getCount *int64, source []T) testInput[T] {
	mutex := new(sync.Mutex)
	return testInput[T]{
		flagGet: func() {
			atomic.AddInt64(getCount, 1)
		},
		get: func() (T, bool) {
			mutex.Lock()
			defer mutex.Unlock()
			if len(source) == 0 {
				var t T
				return t, false
			}
			item := source[0]
			source = source[1:]
			return item, true
		},
	}
}

var _ tract.Output[int64] = testOutput[int64]{}

type testOutput[T tract.Request] struct {
	flagPut   func()
	put       func(T)
	flagClose func()
	close     func()
}

func (o testOutput[T]) Put(t T) {
	o.flagPut()
	o.put(t)
}

func (o testOutput[T]) Close() {
	o.flagClose()
	o.close()
}

func newTestOutput[T tract.Request](putCount, closeCount *int64) testOutput[T] {
	return testOutput[T]{
		flagPut: func() {
			atomic.AddInt64(putCount, 1)
		},
		put: func(T) {},
		flagClose: func() {
			atomic.AddInt64(closeCount, 1)
		},
		close: func() {},
	}
}

func TestWorkerTract(t *testing.T) {
	type (
		myInputType  struct{}
		myOutputType struct{}
	)

	var (
		numberOfInputGets         int64
		numberOfRequestsProcessed int64
		numberOfMadeWorkers       int64
		numberOfWorkersClosed     int64
		numberOfOutputPuts        int64
		numberOfOutputCloses      int64
	)
	// 10 requests
	input := newSourceTestInput(&numberOfInputGets, []myInputType{9: {}})
	workerTract := tract.NewWorkerFactoryTract[myInputType, myOutputType, testWorker[myInputType, myOutputType]]("myWorkerTract", 1, testWorkerFactory[myInputType, myOutputType]{
		flagMakeWorker: func() { numberOfMadeWorkers++ },
		testWorker: testWorker[myInputType, myOutputType]{
			flagClose: func() { numberOfWorkersClosed++ },
			work: func(_ context.Context, _ myInputType) (myOutputType, bool) {
				numberOfRequestsProcessed++
				return myOutputType{}, true
			},
		},
	})
	output := newTestOutput[myOutputType](&numberOfOutputPuts, &numberOfOutputCloses)

	// Pre-Init Checks
	var (
		expectedNumberOfInputGets         int64
		expectedNumberOfRequestsProcessed int64
		expectedNumberOfMadeWorkers       int64
		expectedNumberOfWorkersClosed     int64
		expectedNumberOfOutputPuts        int64
		expectedNumberOfOutputCloses      int64
	)
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	workerTractStarter, err := tract.Init[myInputType, myOutputType](input, workerTract, output)
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfInputGets = 0
	expectedNumberOfRequestsProcessed = 0
	expectedNumberOfMadeWorkers = 1
	expectedNumberOfWorkersClosed = 0
	expectedNumberOfOutputPuts = 0
	expectedNumberOfOutputCloses = 0
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	expectedName := "myWorkerTract"
	actualName := workerTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	workerTractWaiter := workerTractStarter.Start()
	workerTractWaiter.Wait()

	// Finished Checks
	expectedNumberOfInputGets = 11
	expectedNumberOfRequestsProcessed = 10
	expectedNumberOfMadeWorkers = 1
	expectedNumberOfWorkersClosed = 1
	expectedNumberOfOutputPuts = 10
	expectedNumberOfOutputCloses = 1
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}
}

func TestSerialGroupTract(t *testing.T) {
	type (
		myInputType  struct{}
		myInnerType  struct{}
		myOutputType struct{}
	)

	var (
		numberOfInputGets         int64
		numberOfRequestsProcessed = [2]int64{}
		numberOfMadeWorkers       int64
		numberOfWorkersClosed     int64
		numberOfOutputPuts        int64
		numberOfOutputCloses      int64
	)
	// 10 requests
	input := newSourceTestInput(&numberOfInputGets, []myInputType{9: {}})
	myTract := tract.NewSerialGroupTract("mySerialGroupTract",
		tract.NewWorkerFactoryTract[myInputType, myInnerType, testWorker[myInputType, myInnerType]]("head", 1, testWorkerFactory[myInputType, myInnerType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myInputType, myInnerType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, _ myInputType) (myInnerType, bool) {
					atomic.AddInt64(&numberOfRequestsProcessed[0], 1)
					return myInnerType{}, true
				},
			},
		}),
		tract.NewWorkerFactoryTract[myInnerType, myOutputType, testWorker[myInnerType, myOutputType]]("tail", 2, testWorkerFactory[myInnerType, myOutputType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myInnerType, myOutputType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, _ myInnerType) (myOutputType, bool) {
					atomic.AddInt64(&numberOfRequestsProcessed[1], 1)
					return myOutputType{}, true
				},
			},
		}),
	)
	output := newTestOutput[myOutputType](&numberOfOutputPuts, &numberOfOutputCloses)

	// Pre-Init Checks
	var (
		expectedNumberOfInputGets         int64 = 0
		expectedNumberOfRequestsProcessed       = [2]int64{0, 0}
		expectedNumberOfMadeWorkers       int64 = 0
		expectedNumberOfWorkersClosed     int64 = 0
		expectedNumberOfOutputPuts        int64 = 0
		expectedNumberOfOutputCloses      int64 = 0
	)
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	myTractStarter, err := tract.Init[myInputType, myOutputType](input, myTract, output)
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfInputGets = 0
	expectedNumberOfRequestsProcessed = [2]int64{0, 0}
	expectedNumberOfMadeWorkers = 3
	expectedNumberOfWorkersClosed = 0
	expectedNumberOfOutputPuts = 0
	expectedNumberOfOutputCloses = 0
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	expectedName := "mySerialGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTractStarter.Start().Wait()

	// Finished Checks
	expectedNumberOfInputGets = 11
	expectedNumberOfRequestsProcessed = [2]int64{10, 10}
	expectedNumberOfMadeWorkers = 3
	expectedNumberOfWorkersClosed = 3
	expectedNumberOfOutputPuts = 10
	expectedNumberOfOutputCloses = 1
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfRequestsProcessed != expectedNumberOfRequestsProcessed {
		t.Errorf(`number of requests processed: expected %d, received %d`, expectedNumberOfRequestsProcessed, numberOfRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}
}

func TestParalellGroupTract(t *testing.T) {
	type (
		myRequestType struct{}
	)

	var (
		numberOfInputGets                 int64
		numberOfParalellRequestsProcessed = [3]int64{}
		numberOfMadeWorkers               int64
		numberOfWorkersClosed             int64
		numberOfOutputPuts                int64
		numberOfOutputCloses              int64
	)
	// 100 requests
	input := newSourceTestInput(&numberOfInputGets, []myRequestType{99: {}})
	myTract := tract.NewParalellGroupTract[myRequestType, myRequestType]("myParalellGroupTract",
		tract.NewWorkerFactoryTract[myRequestType, myRequestType, testWorker[myRequestType, myRequestType]]("middle1", 1, testWorkerFactory[myRequestType, myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myRequestType, myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, _ myRequestType) (myRequestType, bool) {
					atomic.AddInt64(&numberOfParalellRequestsProcessed[0], 1)
					return myRequestType{}, true
				},
			},
		}),
		tract.NewWorkerFactoryTract[myRequestType, myRequestType, testWorker[myRequestType, myRequestType]]("middle2", 2, testWorkerFactory[myRequestType, myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myRequestType, myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, _ myRequestType) (myRequestType, bool) {
					atomic.AddInt64(&numberOfParalellRequestsProcessed[1], 1)
					return myRequestType{}, true
				},
			},
		}),
		tract.NewWorkerFactoryTract[myRequestType, myRequestType, testWorker[myRequestType, myRequestType]]("middle3", 4, testWorkerFactory[myRequestType, myRequestType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myRequestType, myRequestType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, _ myRequestType) (myRequestType, bool) {
					atomic.AddInt64(&numberOfParalellRequestsProcessed[2], 1)
					return myRequestType{}, true
				},
			},
		}),
	)
	output := newTestOutput[myRequestType](&numberOfOutputPuts, &numberOfOutputCloses)

	// Pre-Init Checks
	var (
		expectedNumberOfInputGets                 int64 = 0
		expectedNumberOfParalellRequestsProcessed int64 = 0
		expectedNumberOfMadeWorkers               int64 = 0
		expectedNumberOfWorkersClosed             int64 = 0
		expectedNumberOfOutputPuts                int64 = 0
		expectedNumberOfOutputCloses              int64 = 0
	)
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	var totalNumberOfParalellRequestsProcessed int64
	for _, n := range numberOfParalellRequestsProcessed {
		totalNumberOfParalellRequestsProcessed += n
	}
	if totalNumberOfParalellRequestsProcessed != expectedNumberOfParalellRequestsProcessed {
		t.Errorf(`number of paralell requests processed: expected %d, received %d`, expectedNumberOfParalellRequestsProcessed, totalNumberOfParalellRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	myTractStarter, err := tract.Init[myRequestType, myRequestType](input, myTract, output)
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfInputGets = 0
	expectedNumberOfParalellRequestsProcessed = 0
	expectedNumberOfMadeWorkers = 7
	expectedNumberOfWorkersClosed = 0
	expectedNumberOfOutputPuts = 0
	expectedNumberOfOutputCloses = 0

	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	totalNumberOfParalellRequestsProcessed = 0
	for _, n := range numberOfParalellRequestsProcessed {
		totalNumberOfParalellRequestsProcessed += n
	}
	if totalNumberOfParalellRequestsProcessed != expectedNumberOfParalellRequestsProcessed {
		t.Errorf(`number of paralell requests processed: expected %d, received %d`, expectedNumberOfParalellRequestsProcessed, totalNumberOfParalellRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	expectedName := "myParalellGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTractStarter.Start().Wait()

	// Finished Checks
	expectedNumberOfInputGets = 107 // 100 requests + 7 total worker in the paralell group
	expectedNumberOfParalellRequestsProcessed = 100
	expectedNumberOfMadeWorkers = 7
	expectedNumberOfWorkersClosed = 7
	expectedNumberOfOutputPuts = 100
	expectedNumberOfOutputCloses = 1

	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	totalNumberOfParalellRequestsProcessed = 0
	for _, n := range numberOfParalellRequestsProcessed {
		totalNumberOfParalellRequestsProcessed += n
	}
	if totalNumberOfParalellRequestsProcessed != expectedNumberOfParalellRequestsProcessed {
		t.Errorf(`number of paralell requests processed: expected %d, received %d`, expectedNumberOfParalellRequestsProcessed, totalNumberOfParalellRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}
}

func TestFanOutGroupTract(t *testing.T) {
	type (
		myInputType  struct{}
		myInnerType  struct{}
		myOutputType struct{}
	)

	var (
		numberOfInputGets               int64
		numberOfHeadRequestsProcessed   int64
		numberOfFanOutRequestsProcessed = [3]int64{}
		numberOfMadeWorkers             int64
		numberOfWorkersClosed           int64
		numberOfOutputPuts              int64
		numberOfOutputCloses            int64
	)
	// 100 requests
	input := newSourceTestInput(&numberOfInputGets, []myInputType{99: {}})
	myTract := tract.NewFanOutGroupTract[myInputType, myInnerType, myOutputType]("myFanOutGroupTract",
		tract.NewWorkerFactoryTract[myInputType, myInnerType, testWorker[myInputType, myInnerType]]("head", 1, testWorkerFactory[myInputType, myInnerType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myInputType, myInnerType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, r myInputType) (myInnerType, bool) {
					atomic.AddInt64(&numberOfHeadRequestsProcessed, 1)
					return myInnerType{}, true
				},
			},
		}),
		tract.NewWorkerFactoryTract[myInnerType, myOutputType, testWorker[myInnerType, myOutputType]]("middle1", 2, testWorkerFactory[myInnerType, myOutputType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myInnerType, myOutputType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, r myInnerType) (myOutputType, bool) {
					atomic.AddInt64(&numberOfFanOutRequestsProcessed[0], 1)
					return myOutputType{}, true
				},
			},
		}),
		tract.NewWorkerFactoryTract[myInnerType, myOutputType, testWorker[myInnerType, myOutputType]]("middle2", 4, testWorkerFactory[myInnerType, myOutputType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myInnerType, myOutputType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, r myInnerType) (myOutputType, bool) {
					atomic.AddInt64(&numberOfFanOutRequestsProcessed[1], 1)
					return myOutputType{}, true
				},
			},
		}),
		tract.NewWorkerFactoryTract[myInnerType, myOutputType, testWorker[myInnerType, myOutputType]]("middle3", 8, testWorkerFactory[myInnerType, myOutputType]{
			flagMakeWorker: func() { atomic.AddInt64(&numberOfMadeWorkers, 1) },
			testWorker: testWorker[myInnerType, myOutputType]{
				flagClose: func() { atomic.AddInt64(&numberOfWorkersClosed, 1) },
				work: func(_ context.Context, r myInnerType) (myOutputType, bool) {
					atomic.AddInt64(&numberOfFanOutRequestsProcessed[2], 1)
					return myOutputType{}, true
				},
			},
		}),
	)
	output := newTestOutput[myOutputType](&numberOfOutputPuts, &numberOfOutputCloses)

	// Pre-Init Checks
	var (
		expectedNumberOfInputGets               int64 = 0
		expectedNumberOfHeadRequestsProcessed   int64 = 0
		expectedNumberOfFanOutRequestsProcessed       = [3]int64{0, 0, 0}
		expectedNumberOfMadeWorkers             int64 = 0
		expectedNumberOfWorkersClosed           int64 = 0
		expectedNumberOfOutputPuts              int64 = 0
		expectedNumberOfOutputCloses            int64 = 0
	)
	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfHeadRequestsProcessed != expectedNumberOfHeadRequestsProcessed {
		t.Errorf(`number of head requests processed: expected %d, received %d`, expectedNumberOfHeadRequestsProcessed, numberOfHeadRequestsProcessed)
	}
	if numberOfFanOutRequestsProcessed != expectedNumberOfFanOutRequestsProcessed {
		t.Errorf(`number of fan out requests processed: expected %d, received %d`, expectedNumberOfFanOutRequestsProcessed, numberOfFanOutRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	myTractStarter, err := tract.Init[myInputType, myOutputType](input, myTract, output)
	if err != nil {
		t.Errorf("unexpected error during tract initialization %v", err)
	}

	// Pre-Start Checks
	expectedNumberOfInputGets = 0
	expectedNumberOfHeadRequestsProcessed = 0
	expectedNumberOfFanOutRequestsProcessed = [3]int64{0, 0, 0}
	expectedNumberOfMadeWorkers = 15
	expectedNumberOfWorkersClosed = 0
	expectedNumberOfOutputPuts = 0
	expectedNumberOfOutputCloses = 0

	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfHeadRequestsProcessed != expectedNumberOfHeadRequestsProcessed {
		t.Errorf(`number of head requests processed: expected %d, received %d`, expectedNumberOfHeadRequestsProcessed, numberOfHeadRequestsProcessed)
	}
	if numberOfFanOutRequestsProcessed != expectedNumberOfFanOutRequestsProcessed {
		t.Errorf(`number of fan out requests processed: expected %d, received %d`, expectedNumberOfFanOutRequestsProcessed, numberOfFanOutRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}

	expectedName := "myFanOutGroupTract"
	actualName := myTract.Name()
	if actualName != expectedName {
		t.Errorf("name: expected %q, received %q", expectedName, actualName)
	}

	myTractStarter.Start().Wait()

	// Finished Checks
	expectedNumberOfInputGets = 101
	expectedNumberOfHeadRequestsProcessed = 100
	expectedNumberOfFanOutRequestsProcessed = [3]int64{100, 100, 100}
	expectedNumberOfMadeWorkers = 15
	expectedNumberOfWorkersClosed = 15
	expectedNumberOfOutputPuts = 300
	expectedNumberOfOutputCloses = 1

	if numberOfInputGets != expectedNumberOfInputGets {
		t.Errorf(`number of input requests gotten: expected %d, received %d`, expectedNumberOfInputGets, numberOfInputGets)
	}
	if numberOfHeadRequestsProcessed != expectedNumberOfHeadRequestsProcessed {
		t.Errorf(`number of head requests processed: expected %d, received %d`, expectedNumberOfHeadRequestsProcessed, numberOfHeadRequestsProcessed)
	}
	if numberOfFanOutRequestsProcessed != expectedNumberOfFanOutRequestsProcessed {
		t.Errorf(`number of fan out requests processed: expected %d, received %d`, expectedNumberOfFanOutRequestsProcessed, numberOfFanOutRequestsProcessed)
	}
	if numberOfMadeWorkers != expectedNumberOfMadeWorkers {
		t.Errorf(`number of made workers: expected %d, received %d`, expectedNumberOfMadeWorkers, numberOfMadeWorkers)
	}
	if numberOfWorkersClosed != expectedNumberOfWorkersClosed {
		t.Errorf(`number of worker closures: expected %d, received %d`, expectedNumberOfWorkersClosed, numberOfWorkersClosed)
	}
	if numberOfOutputPuts != expectedNumberOfOutputPuts {
		t.Errorf(`number of output puts: expected %d, received %d`, expectedNumberOfOutputPuts, numberOfOutputPuts)
	}
	if numberOfOutputCloses != expectedNumberOfOutputCloses {
		t.Errorf(`number of output closures: expected %d, received %d`, expectedNumberOfOutputCloses, numberOfOutputCloses)
	}
}
