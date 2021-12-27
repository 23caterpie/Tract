package octract

// import (
// 	"time"

// 	tract "github.com/23caterpie/Tract"

// 	"go.opencensus.io/stats"
// 	"go.opencensus.io/tag"
// )

// var (
// 	_ tract.Worker[tract.Request, tract.Request] = WorkerMiddleware{}
// 	_ tract.WorkerMiddleware = WrapWorker
// )

// func WrapWorker(
// 	workerContext tract.WorkerContext,
// 	base tract.Worker[tract.Request, tract.Request],
// ) tract.Worker[tract.Request, tract.Request] {
// 	return WorkerMiddleware{
// 		workerName: workerContext.WorkerName,
// 		base:       base,
// 	}
// }

// type WorkerMiddleware struct {
// 	workerName string
// 	base       tract.Worker[tract.Request, tract.Request]
// }

// func (m WorkerMiddleware) Work(inputRequest tract.Request) (tract.Request, bool) {
// 	// Start trace span.
// 	endSpan := startSpan(inputRequest, makeMiddlewareSpanName(m.workerName, CheckpointTypeWork))
// 	// Take start time for stats.
// 	start := time.Now()

// 	// Do work.
// 	outputRequest, ok := m.base.Work(inputRequest)

// 	// Measure work duration.
// 	stats.RecordWithTags(getCtx(outputRequest),
// 		[]tag.Mutator{
// 			tag.Upsert(WorkerName, m.workerName),
// 		},
// 		WorkerLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
// 	)
// 	// End trace span.
// 	endSpan()

// 	return outputRequest, ok
// }

// var _ tract.Input[int64] = InputMiddleware[int64]{}

// func WrapInput[T tract.Request](
// 	workerName string,
// 	base tract.Input[T],
// ) InputMiddleware[T] {
// 	return InputMiddleware[T]{
// 		workerName: workerName,
// 		base:       base,
// 	}
// }

// type InputMiddleware[T tract.Request] struct {
// 	workerName string
// 	base       tract.Input[T]
// }

// func (m InputMiddleware[T]) Get() (T, bool) {
// 	// Take start time for stats.
// 	start := time.Now()

// 	// Get Request
// 	req, ok := m.base.Get()

// 	// Measure get duration.
// 	stats.RecordWithTags(getCtx(req),
// 		[]tag.Mutator{
// 			tag.Upsert(WorkerName, m.workerName),
// 		},
// 		InputLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
// 	)
// 	// TODO: use last output time on context to get a request wait time.

// 	return req, ok
// }

// var _ tract.Output[int64] = OutputMiddleware[int64]{}

// type OutputMiddleware[T tract.Request] struct {
// 	workerName string
// 	base       tract.Output[T]
// }

// func (m OutputMiddleware[T]) Put(req T) {
// 	// Take start time for stats.
// 	start := time.Now()
// 	// TODO: Attach output start time to req context

// 	// Put Request
// 	m.base.Put(req)

// 	// Measure put duration.
// 	stats.RecordWithTags(getCtx(req),
// 		[]tag.Mutator{
// 			tag.Upsert(WorkerName, m.workerName),
// 		},
// 		OutputLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
// 	)
// }

// func (m OutputMiddleware[T]) Close() {
// 	m.base.Close()
// }
