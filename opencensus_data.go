package tract

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

func newOpencensusData(ctx context.Context) opencensusData {
	return opencensusData{
		baseData: newOpencensusUnitData(ctx, `octract/base`, now()),
	}
}

type opencensusData struct {
	// The below opencensus unit data stacks are used for metrics and tracing.
	// They are used to replicate a stack frame of data that would be passed if each sibling tract were called in sequence
	// and tracts inside of group tracts were nested functions.
	// This can be used to show the lifecycle of a request processing through a tract through the perspective of the request itself
	// allowing spans to have the same parent child relationships they would have in a non concurrent environment.

	// baseData is opencensusUnitData that is set on request wrapper input and used on request wrapper output.
	// It serves as the base data all other stack data is based on.
	// If a BaseContext function is being used in the request wrapper input, then its context will be what it specifies,
	// so deadlines, values, or anything on the context will be passed to workers as well.
	baseData opencensusUnitData
	// inputDataStack is a stack of opencensusUnitData that is pushed to on input and popped from on outputs.
	inputDataStack []opencensusUnitData
	// outputDataStack is a stack of opencensusUnitData that is pushed to on output and emptied from on inputs.
	outputDataStack []opencensusUnitData
}

// context gets the context for the most recently pushed input stack frame.
// If there are none, then the base context is returned.
func (d opencensusData) context() context.Context {
	if len(d.inputDataStack) > 0 {
		return d.inputDataStack[len(d.inputDataStack)-1].ctx
	}
	return d.baseData.ctx
}

func (d *opencensusData) popInputData() time.Time {
	if len(d.inputDataStack) == 0 {
		return time.Time{}
	}
	var data opencensusUnitData
	d.inputDataStack, data = d.inputDataStack[:len(d.inputDataStack)-1], d.inputDataStack[len(d.inputDataStack)-1]
	data.endSpan()
	return data.timestamp
}

func (d *opencensusData) pushInputData(
	ctx context.Context,
	spanName string,
	afterGet time.Time,
) {
	d.inputDataStack = append(d.inputDataStack, newOpencensusUnitData(ctx, spanName, afterGet))
}

func (d *opencensusData) popAllOutputData() time.Time {
	var ts time.Time
	for i := len(d.outputDataStack) - 1; i >= 0; i-- {
		data := d.outputDataStack[i]
		ts = data.timestamp
		data.endSpan()
	}
	d.outputDataStack = nil
	return ts
}

func (d *opencensusData) pushOutputData(
	ctx context.Context,
	spanName string,
	beforePut time.Time,
) {
	d.outputDataStack = append(d.outputDataStack, newOpencensusUnitData(ctx, spanName, beforePut))
}

func (d opencensusData) clone(amount int32) []opencensusData {
	inputBlockCounts := make([]int32, len(d.inputDataStack))
	for i := range inputBlockCounts {
		inputBlockCounts[i] = amount
	}
	outputBlockCounts := make([]int32, len(d.outputDataStack))
	for i := range outputBlockCounts {
		outputBlockCounts[i] = amount
	}
	
	clones := make([]opencensusData, amount)
	for i := range clones {
		if i == len(clones) - 1 {
			d.baseData.blockEndSpan(&amount)
			blockOpencensusUnitDataStack(d.inputDataStack, inputBlockCounts)
			blockOpencensusUnitDataStack(d.outputDataStack, outputBlockCounts)
			clones[i] = d
		} else {
			baseData := d.baseData
			baseData.blockEndSpan(&amount)
			clones[i] = opencensusData{
				baseData:        baseData,
				inputDataStack:  cloneOpencensusUnitDataStack(d.inputDataStack, inputBlockCounts),
				outputDataStack: cloneOpencensusUnitDataStack(d.outputDataStack, outputBlockCounts),
			}
		}
	}

	return clones
}

func cloneOpencensusUnitDataStack(stack []opencensusUnitData, amounts []int32) []opencensusUnitData {
	newStack := make([]opencensusUnitData, len(stack))
	copy(newStack, stack)
	blockOpencensusUnitDataStack(newStack, amounts)
	return newStack
}

func blockOpencensusUnitDataStack(stack []opencensusUnitData, amounts []int32) {
	for i := range stack {
		stack[i].blockEndSpan(&amounts[i])
	}
}

func newOpencensusUnitData(
	ctx context.Context,
	spanName string,
	timestamp time.Time,
) opencensusUnitData {
	ctx, span := trace.StartSpan(ctx, spanName)
	return opencensusUnitData{
		ctx:       ctx,
		timestamp: timestamp,
		endSpan:   span.End,
	}
}

// opencensusUnitData is data kept track of on an input or output.
// ctx is a context that is potentially passed to worker Work() calls.
//   it has all the characteristics of the base context of the tract
//   plus contains opencensus spans created by the tract.
// timestamp is a moment in time.
//   For inputs it's the moment upon getting the request from Get().
//   For outputs it's the moment right before calling Put() on the request.
//   The different between a Get() and the next Put() is how much time was spent in the tract.
//   The different between a Put() and the next Get() is how much time was spend waiting between tracts.
// endSpan is a function that ends a tracing span.
//   Inputs' endSpan should be called at the start of an output to show the time inbetween as "work".
//   Outputs' endSpan shoudl be called right after getting the request in an input to show the time inbetween as "wait".
type opencensusUnitData struct {
	ctx       context.Context
	timestamp time.Time
	endSpan   func()
}

func (d *opencensusUnitData) blockEndSpan(amount *int32) {
	var (
		endSpan   = d.endSpan
	)
	d.endSpan = func() {
		newAmount := atomic.AddInt32(amount, -1)
		if newAmount <= 0 {
			endSpan()
		}
	}
}

var (
	now = time.Now
)

func newOpencensusWorkerLinks[T, D Request](
	workerName string,
	baseInput Input[RequestWrapper[T]],
	baseOutput Output[RequestWrapper[D]],
) (
	Input[RequestWrapper[T]],
	Output[RequestWrapper[D]],
) {
	if workerName == "" {
		return baseInput, baseOutput
	}
	return newOpencensusWorkerInput(workerName, baseInput),
		newOpencensusWorkerOutput(workerName, baseOutput)
}

func newOpencensusGroupLinks[T, D Request](
	groupName string,
	baseInput Input[RequestWrapper[T]],
	baseOutput Output[RequestWrapper[D]],
) (
	Input[RequestWrapper[T]],
	Output[RequestWrapper[D]],
) {
	if groupName == "" {
		return baseInput, baseOutput
	}
	return newOpencensusGroupInput(groupName, baseInput),
		newOpencensusGroupOutput(groupName, baseOutput)
}

var (
	_ Input[RequestWrapper[int]]  = opencensusInput[int]{}
	_ Output[RequestWrapper[int]] = opencensusOutput[int]{}
)

func newOpencensusWorkerInput[T Request](
	workerName string,
	base Input[RequestWrapper[T]],
) Input[RequestWrapper[T]] {
	return opencensusInput[T]{
		base:         base,
		inputLatency: WorkerInputLatency,
		waitLatency:  WorkerWaitLatency,
		tags: []tag.Mutator{
			tag.Upsert(WorkerName, workerName),
		},
		spanName: fmt.Sprintf(`octract/worker/%s/work`, workerName),
	}
}

func newOpencensusGroupInput[T Request](
	groupName string,
	base Input[RequestWrapper[T]],
) Input[RequestWrapper[T]] {
	return opencensusInput[T]{
		base:         base,
		inputLatency: GroupInputLatency,
		waitLatency:  GroupWaitLatency,
		tags: []tag.Mutator{
			tag.Upsert(GroupName, groupName),
		},
		spanName: fmt.Sprintf(`octract/group/%s/work`, groupName),
	}
}

type opencensusInput[T Request] struct {
	base         Input[RequestWrapper[T]]
	inputLatency *stats.Float64Measure
	waitLatency  *stats.Float64Measure
	tags         []tag.Mutator
	spanName     string
}

func (i opencensusInput[T]) Get() (RequestWrapper[T], bool) {
	start := now()
	req, ok := i.base.Get()
	end := now()
	if !ok {
		return req, ok
	}
	var (
		// Our context is is the most recent group tract we're in, or the base of the whole tract
		ctx          = req.meta.opencensusData.context()
		measurements = []stats.Measurement{
			// Populate input latency.
			i.inputLatency.M(float64(end.Sub(start)) / float64(time.Millisecond)),
		}
	)
	// Populate wait latency using last output time.
	if outputTime := req.meta.opencensusData.popAllOutputData(); !outputTime.IsZero() {
		measurements = append(measurements,
			i.waitLatency.M(float64(end.Sub(outputTime))/float64(time.Millisecond)),
		)
	}
	stats.RecordWithTags(ctx, i.tags, measurements...)
	req.meta.opencensusData.pushInputData(ctx, i.spanName, end)
	return req, ok
}

func newOpencensusWorkerOutput[T Request](
	workerName string,
	base Output[RequestWrapper[T]],
) Output[RequestWrapper[T]] {
	return opencensusOutput[T]{
		base:          base,
		workLatency:   WorkerWorkLatency,
		outputLatency: WorkerOutputLatency,
		tags: []tag.Mutator{
			tag.Upsert(WorkerName, workerName),
		},
		spanName: fmt.Sprintf(`octract/worker/%s/wait`, workerName),
	}
}

func newOpencensusGroupOutput[T Request](
	groupName string,
	base Output[RequestWrapper[T]],
) Output[RequestWrapper[T]] {
	return opencensusOutput[T]{
		base:          base,
		workLatency:   GroupWorkLatency,
		outputLatency: GroupOutputLatency,
		tags: []tag.Mutator{
			tag.Upsert(GroupName, groupName),
		},
		spanName: fmt.Sprintf(`octract/group/%s/wait`, groupName),
	}
}

type opencensusOutput[T Request] struct {
	base          Output[RequestWrapper[T]]
	workLatency   *stats.Float64Measure
	outputLatency *stats.Float64Measure
	tags          []tag.Mutator
	spanName      string
}

func (i opencensusOutput[T]) Put(req RequestWrapper[T]) {
	var (
		// Input data must be popped strictly before we get the context, since that will change the context we use.
		inputTime = req.meta.opencensusData.popInputData()
		// Our context is the most recent group tract we're in, or the base of the whole tract
		ctx = req.meta.opencensusData.context()
	)
	start := now()
	req.meta.opencensusData.pushOutputData(ctx, i.spanName, start)
	i.base.Put(req)
	end := now()
	// Once request has been pushed, it must not be modified since another go routine may be using it.

	// Take measurement based off the data we gathered above.
	// Do it down here so we can get the Put() call through as early as possible.
	measurements := []stats.Measurement{
		// Populate output latency.
		i.outputLatency.M(float64(end.Sub(start)) / float64(time.Millisecond)),
	}
	// Populate work latency using last input time.
	// "work" is time spent calling Work() in a worker tract or the cumulative time spent in a group tract.
	if !inputTime.IsZero() {
		measurements = append(measurements,
			i.workLatency.M(float64(start.Sub(inputTime))/float64(time.Millisecond)),
		)
	}
	stats.RecordWithTags(ctx, i.tags, measurements...)
}

func (i opencensusOutput[T]) Close() {
	i.base.Close()
}
