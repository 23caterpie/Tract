package tract

import (
	"fmt"
	"sync"
)

// NewWorkerTract makes a new tract that will spin up @size number of workers generated from @workerFactory
// that get from the input and put to the output of the tract.
func NewWorkerTract[InputType, OutputType Request](
	name string,
	size int,
	workerFactory WorkerFactory[InputType, OutputType],
) Tract[InputType, OutputType] {
	p := &workerTract[InputType, OutputType]{
		factory: workerFactory,
		name:    name,
		size:    size,
	}
	return p
}

type workerTract[InputType, OutputType Request] struct {
	// NewWorkerTract() contructor initilized fields

	// Name of the Tract: used for logging and instrementation
	name string
	// Amount of workers to start
	size int
	// Factory that makes the workers on demand
	factory WorkerFactory[InputType, OutputType]
}

func (p *workerTract[InputType, OutputType]) Name() string {
	return p.name
}

func (p *workerTract[InputType, OutputType]) Init(
	input Input[InputType],
	output Output[OutputType],
) (TractStarter, error) {
	return newInitializedWorkerTract(
		p.name,
		p.size,
		p.factory,
		input,
		output,
	)
}

func newInitializedWorkerTract[InputType, OutputType Request](
	name string,
	size int,
	factory WorkerFactory[InputType, OutputType],
	input Input[InputType],
	output Output[OutputType],
) (*initializedWorkerTract[InputType, OutputType], error) {
	// Make all the  workers.
	p := initializedWorkerTract[InputType, OutputType]{
		name:    name,
		input:   input,
		output:  output,
		workers: make([]WorkerCloser[InputType, OutputType], size),
	}
	var err error
	for i := range p.workers {
		p.workers[i], err = factory.MakeWorker()
		if err != nil {
			p.closeWorkers()
			return nil, fmt.Errorf("failed to make worker[%d]: %w", i, err)
		}
	}

	return &p, nil
}

type initializedWorkerTract[InputType, OutputType Request] struct {
	name string
	// Input used by all workers
	input Input[InputType]
	// Output used by all workers
	output  Output[OutputType]
	workers []WorkerCloser[InputType, OutputType]
}

func (p *initializedWorkerTract[InputType, OutputType]) Start() TractWaiter {
	// Start all the processors
	workerWG := &sync.WaitGroup{}
	for i := range p.workers {
		workerWG.Add(1)
		go func(worker Worker[InputType, OutputType]) {
			defer workerWG.Done()
			process(
				p.input,
				worker,
				p.output,
			)
		}(p.workers[i])
	}
	// Automatically close all the workers and the output when all the workers finish.
	return tractWaiterFunc(func() {
		workerWG.Wait()
		p.close()
	})
}

func (p *initializedWorkerTract[InputType, OutputType]) close() {
	p.closeWorkers()
	if p.output != nil {
		p.output.Close()
	}
}

func (p *initializedWorkerTract[InputType, OutputType]) closeWorkers() {
	for i := range p.workers {
		if worker := p.workers[i]; worker != nil {
			worker.Close()
		}
	}
}

func process[InputType, OutputType Request](
	input Input[InputType],
	worker Worker[InputType, OutputType],
	output Output[OutputType],
) {
	var (
		outputRequest OutputType
		shouldSend    bool

		inputRequest InputType
		ok           bool
	)
	for {
		inputRequest, ok = input.Get()
		if !ok {
			break
		}

		outputRequest, shouldSend = worker.Work(inputRequest)

		if shouldSend && output != nil {
			output.Put(outputRequest)
		}
	}
}
