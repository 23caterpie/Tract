package tract

import (
	"sync"
)

// NewWorkerTract makes a new tract that will spin up @size number of workers generated from @workerFactory
// that get from the input and put to the output of the tract.
func NewWorkerTract(name string, size int, workerFactory WorkerFactory, options ...WorkerTractOption) Tract {
	return &workerTract{
		// input and output are overwritten when tracts are linked together
		input:   InputGenerator{},
		output:  FinalOutput{},
		factory: workerFactory,
		name:    name,
		size:    size,
		options: options,
	}
}

type workerTract struct {
	// NewWorkerTract() contructor initilized fields

	// Input used by all workers
	input Input
	// Output used by all workers
	output Output
	// Factory that makes the workers on demand
	factory WorkerFactory
	// Name of the Tract: used for logging and instrementation
	name string
	// Amount of workers to start
	size int
	// Additonal options applied to the tract on startup
	options []WorkerTractOption

	// init() initialized fields

	// Workers
	workers []Worker

	// applyOptions() initialized fields

	// Handler for request latency metrics within each running process in the tract
	metricsHandler MetricsHandler
}

func (p *workerTract) Name() string {
	return p.name
}

func (p *workerTract) Init() error {
	// Close the workers just in case init was called multiple times
	p.closeWorkers()
	// Make all the  workers
	p.workers = make([]Worker, p.size)
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

func (p *workerTract) Start() func() {
	p.applyOptions()
	// Start all the processors
	workerWG := &sync.WaitGroup{}
	for i := range p.workers {
		workerWG.Add(1)
		go func(worker Worker) {
			defer workerWG.Done()
			process(p.input, worker, p.output, p.metricsHandler)
		}(p.workers[i])
	}
	// Automatically close all the workers, the factory, and the output when all the workers finish.
	return func() {
		workerWG.Wait()
		p.close()
	}
}

func (p *workerTract) SetInput(in Input) {
	p.input = in
}

func (p *workerTract) SetOutput(out Output) {
	p.output = out
}

// This is called upon starting the tract; ensuring any changes to input or output has taken place before being called.
func (p *workerTract) applyOptions() {
	for _, option := range p.options {
		option(p)
	}
}

func (p *workerTract) close() {
	for i := range p.workers {
		if worker := p.workers[i]; worker != nil {
			worker.Close()
		}
	}
	p.closeWorkers()
	p.output.Close()
	p.factory.Close()
}

func (p *workerTract) closeWorkers() {
	for i := range p.workers {
		if worker := p.workers[i]; worker != nil {
			worker.Close()
		}
	}
}

func process(input Input, worker Worker, output Output, metricsHandler MetricsHandler) {
	var (
		mh  = &manualOverrideMetricsHandler{MetricsHandler: metricsHandler}
		in  = MetricsInput{Input: input, metricsHandler: mh}
		w   = MetricsWorker{Worker: worker, metricsHandler: mh}
		out = MetricsOutput{Output: output, metricsHandler: mh}

		outputRequest Request
		shouldSend    bool

		inputRequest Request
		ok           bool

		_, isHeadTract = input.(InputGenerator)
	)
	for {
		mh.SetShouldHandle(metricsHandler != nil && metricsHandler.ShouldHandle())
		inputRequest, ok = in.Get()
		if !ok {
			break
		}
		outputRequest, shouldSend = w.Work(inputRequest)
		if shouldSend {
			out.Put(outputRequest)
		} else if isHeadTract {
			// If this is the head tract, then the worker is responsible for termination.
			// If the worker returns a "should not send" result, this is a signal to stop processing.
			break
		}
	}
}
