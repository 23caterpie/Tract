package tract

import (
	"context"
	"fmt"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

type opencensusData struct {
	// // workerData is information about worker tracts used for metrics/tracing.
	// // This data is set on the input to a worker tract and used on the output of
	// // that worker tract.
	// workerData opencensusUnitData
	// // groupDataStack is information about group tracts used for metrics/tracing.
	// // This data is pushed on the stack on the input to a group tract
	// // and popped on the output of that group tract.
	// // This is a stack since groups can contain more groups that will finish before
	// // the parent group finishes.
	// // TODO: can maybe merge workerDataStack and groupDataStack?
	// groupDataStack []opencensusUnitData

	// tracing spans are attached to the context for workers to make child spans off of.
	// If a BaseContext function is being used, then this context will be what it specifies,
	// so deadlines, values, or anything on the context will be passed to workers as well.
	ctx context.Context
	// TODO: document.
	inputDataStack []opencensusUnitData
	// TODO: think about how stacking multiple outputs should work.
	outputDataStack []opencensusUnitData
}

// context gets the context for the most recently pushed input stack frame.
// If there are none, then the base context is returned.
func (d opencensusData) context() context.Context {
	if len(d.inputDataStack) > 0 {
		return d.inputDataStack[len(d.inputDataStack)-1].ctx
	}
	return d.ctx
}

func (d opencensusData) popInputData() time.Time {
	if len(d.inputDataStack) == 0 {
		return time.Time{}
	}
	var data opencensusUnitData
	d.inputDataStack, data = d.inputDataStack[:len(d.inputDataStack)-1], d.inputDataStack[len(d.inputDataStack)-1]
	data.endSpan()
	return data.timestamp
}

func (d opencensusData) pushInputData(
	ctx context.Context,
	spanName string,
	afterGet time.Time,
) {
	d.inputDataStack = append(d.inputDataStack, newOpencensusUnitData(ctx, spanName, afterGet))
}

func (d opencensusData) popOutputData() time.Time {
	if len(d.outputDataStack) == 0 {
		return time.Time{}
	}
	var data opencensusUnitData
	d.outputDataStack, data = d.outputDataStack[:len(d.outputDataStack)-1], d.outputDataStack[len(d.outputDataStack)-1]
	data.endSpan()
	return data.timestamp
}

func (d opencensusData) pushOutputData(
	ctx context.Context,
	spanName string,
	beforePut time.Time,
) {
	d.outputDataStack = append(d.outputDataStack, newOpencensusUnitData(ctx, spanName, beforePut))
}

func (d opencensusData) clone() opencensusData {
	inputDataStack := make([]opencensusUnitData, len(d.inputDataStack))
	copy(inputDataStack, d.inputDataStack)
	outputDataStack := make([]opencensusUnitData, len(d.outputDataStack))
	copy(outputDataStack, d.outputDataStack)
	return opencensusData{
		ctx:             d.ctx,
		inputDataStack:  inputDataStack,
		outputDataStack: outputDataStack,
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
// endSpan ...
type opencensusUnitData struct {
	ctx       context.Context
	timestamp time.Time
	endSpan   func()
}

var (
	now = time.Now
)

// TODO: use these.

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
	if outputTime := req.meta.opencensusData.popOutputData(); !outputTime.IsZero() {
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
	// Do it down here so we can get the Put() call throughas early as possible.
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
