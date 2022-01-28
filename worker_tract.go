package tract

import (
	"fmt"
	"sync"
)

// NewWorkerFactoryTract makes a new tract that will spin up @size number of workers generated from @workerFactory
// that get from the input and put to the output of the tract.
func NewWorkerFactoryTract[InputType, OutputType Request, WorkerType Worker[InputType, OutputType]](
	name string,
	size int,
	workerFactory WorkerFactory[InputType, OutputType, WorkerType],
) Tract[InputType, OutputType] {
	p := &workerTract[InputType, OutputType]{
		factory: newInternalWorkerFactory(workerFactory),
		name:    name,
		size:    size,
	}
	return p
}

// NewWorkerTract makes a new tract that will spin up @size number of @workers
// @worker's Work() method must be  thread safe.
// that get from the input and put to the output of the tract.
func NewWorkerTract[InputType, OutputType Request](
	name string,
	size int,
	worker Worker[InputType, OutputType],
) Tract[InputType, OutputType] {
	return NewWorkerFactoryTract(name, size, NewFactoryFromWorker(worker))
}

type workerTract[InputType, OutputType Request] struct {
	// NewWorkerTract() contructor initilized fields

	// Name of the Tract: used for logging and instrementation
	name string
	// Amount of workers to start
	size int
	// Factory that makes the workers on demand
	factory WorkerFactory[InputType, OutputType, Worker[InputType, OutputType]]
}

func (p *workerTract[InputType, OutputType]) Name() string {
	return p.name
}

func (p *workerTract[InputType, OutputType]) Init(
	input Input[RequestWrapper[InputType]],
	output Output[RequestWrapper[OutputType]],
) (TractStarter, error) {
	input, output = newOpencensusWorkerLinks(p.name, input, output)
	return newInitializedWorkerTract(
		p.size,
		p.factory,
		input,
		output,
	)
}

func newInitializedWorkerTract[InputType, OutputType Request](
	size int,
	factory WorkerFactory[InputType, OutputType, Worker[InputType, OutputType]],
	input Input[RequestWrapper[InputType]],
	output Output[RequestWrapper[OutputType]],
) (*initializedWorkerTract[InputType, OutputType], error) {
	// Make all the  workers.
	p := initializedWorkerTract[InputType, OutputType]{
		input:   input,
		output:  output,
		workers: make([]Worker[InputType, OutputType], size),
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
	// Input used by all workers
	input Input[RequestWrapper[InputType]]
	// Output used by all workers
	output  Output[RequestWrapper[OutputType]]
	workers []Worker[InputType, OutputType]
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
		switch worker := p.workers[i].(type) {
		case Closer:
			worker.Close()
		}
	}
}

func process[InputType, OutputType Request](
	input Input[RequestWrapper[InputType]],
	worker Worker[InputType, OutputType],
	output Output[RequestWrapper[OutputType]],
) {
	var (
		outputRequest OutputType
		shouldSend    bool

		inputRequest RequestWrapper[InputType]
		ok           bool

		deadLetterOutput = newRequestWrapperOutput(newNoopOutput[OutputType]())
	)
	for {
		inputRequest, ok = input.Get()
		if !ok {
			break
		}

		outputRequest, shouldSend = worker.Work(
			inputRequest.meta.opencensusData.context(),
			inputRequest.base,
		)
		outputRequestWrapper := newRequestWrapper(outputRequest, inputRequest.meta)

		if shouldSend && output != nil {
			output.Put(outputRequestWrapper)
		} else {
			// Does final operations on the base of the request wrapper.
			// TODO: test this case.
			deadLetterOutput.Put(outputRequestWrapper)
		}
	}
}
