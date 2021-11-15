package tract

import (
	"sync"
)

// NewWorkerTract makes a new tract that will spin up @size number of workers generated from @workerFactory
// that get from the input and put to the output of the tract.
func NewWorkerTract[T any](name string, size int, workerFactory WorkerFactory[T], options ...WorkerTractOption[T]) Tract[T] {
	return &workerTract[T]{
		// input and output are overwritten when tracts are linked together
		input:              InputGenerator[T]{},
		output:             FinalOutput[T]{},
		factory:            workerFactory,
		name:               name,
		size:               size,
		options:            options,
		shouldCloseFactory: false,
	}
}

type workerTract[T any] struct {
	// NewWorkerTract() contructor initilized fields

	// Input used by all workers
	input Input[T]
	// Output used by all workers
	output Output[T]
	// Factory that makes the workers on demand
	factory WorkerFactory[T]
	// Name of the Tract: used for logging and instrementation
	name string
	// Amount of workers to start
	size int
	// Additonal options applied to the tract on startup
	options []WorkerTractOption[T]

	// init() initialized fields

	// Workers
	workers []Worker[T]

	// applyOptions() initialized fields

	shouldCloseFactory bool
}

func (p *workerTract[_]) Name() string {
	return p.name
}

func (p *workerTract[T]) Init() error {
	// Close the workers just in case init was called multiple times
	p.closeWorkers()
	// Make all the  workers
	p.workers = make([]Worker[T], p.size)
	var err error
	for i := range p.workers {
		p.workers[i], err = p.factory.MakeWorker()
		if err != nil {
			p.close()
			return err
		}
	}
	return nil
}

func (p *workerTract[T]) Start() func() {
	p.applyOptions()
	// Start all the processors
	workerWG := &sync.WaitGroup{}
	for i := range p.workers {
		workerWG.Add(1)
		go func(worker Worker[T]) {
			defer workerWG.Done()
			process(p.input, worker, p.output)
		}(p.workers[i])
	}
	// Automatically close all the workers, the factory, and the output when all the workers finish.
	return func() {
		workerWG.Wait()
		p.close()
	}
}

func (p *workerTract[T]) SetInput(in Input[T]) {
	p.input = in
}

func (p *workerTract[T]) SetOutput(out Output[T]) {
	p.output = out
}

// This is called upon starting the tract; ensuring any changes to input or output has taken place before being called.
func (p *workerTract[_]) applyOptions() {
	for _, option := range p.options {
		option(p)
	}
}

func (p *workerTract[_]) close() {
	p.closeWorkers()
	p.output.Close()
	if p.shouldCloseFactory {
		p.factory.Close()
	}
}

func (p *workerTract[_]) closeWorkers() {
	for i := range p.workers {
		if worker := p.workers[i]; worker != nil {
			worker.Close()
		}
	}
}

func process[T any](input Input[T], worker Worker[T], output Output[T]) {
	var (
		outputRequest *Request[T]
		shouldSend    bool

		inputRequest *Request[T]
		ok           bool

		_, isHeadTract = input.(InputGenerator[T])
	)
	for {
		inputRequest, ok = input.Get()
		if !ok {
			break
		}
		outputRequest, shouldSend = worker.Work(inputRequest)
		if shouldSend {
			output.Put(outputRequest)
		} else {
			if isHeadTract {
				// If this is the head tract, then the worker is responsible for termination.
				// If the worker returns a "should not send" result, this is the signal to stop processing.
				break
			}
		}
	}
}
