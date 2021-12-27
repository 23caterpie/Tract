package octract

import (
	"context"
	"time"

	tract "github.com/23caterpie/Tract"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

func workerWorkCheckpoint(workerContext tract.WorkerContext, inputRequest tract.Request) tract.WorkerWorkCheckpointClosure {
	// Start trace span.
	endSpan := startSpan(inputRequest, makeCheckpointSpanName(TractTypeWorker, workerContext.WorkerName, CheckpointTypeWork))
	// Take start time for stats.
	start := now()
	return func(outputRequest tract.Request) {
		ctx, setCtx := getCtx(outputRequest)
		defer setCtx(ctx)
		// Measure work duration.
		stats.RecordWithTags(ctx,
			[]tag.Mutator{
				tag.Upsert(WorkerName, workerContext.WorkerName),
			},
			WorkerWorkLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
		)
		// End trace span.
		endSpan()
	}
}

func workerInputCheckpoint(workerContext tract.WorkerContext) tract.InputCheckpointClosure {
	// Take start time for stats.
	start := now()
	return func(req tract.Request) {
		ctx, setCtx := getCtx(req)
		defer setCtx(ctx)
		// Measure get duration.
		stats.RecordWithTags(ctx,
			[]tag.Mutator{
				tag.Upsert(WorkerName, workerContext.WorkerName),
			},
			WorkerInputLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
		)
		// Use last output time on context to get a request wait time.
		if waitStart := getRequestCheckpointWorkerWaitStartTime(req); waitStart != nil {
			stats.RecordWithTags(ctx,
				[]tag.Mutator{
					tag.Upsert(WorkerName, workerContext.WorkerName),
				},
				WorkerWaitLatency.M(float64(time.Since(*waitStart))/float64(time.Millisecond)),
			)
		}
	}
}

func workerOutputCheckpoint(workerContext tract.WorkerContext, req tract.Request) tract.OutputCheckpointClosure {
	// Take start time for stats.
	start := now()
	// Attach output start time to req context
	setRequestCheckpointWorkerWaitStartTime(req, start)
	return func() {
		ctx, setCtx := getCtx(req)
		defer setCtx(ctx)
		// Measure put duration.
		stats.RecordWithTags(ctx,
			[]tag.Mutator{
				tag.Upsert(WorkerName, workerContext.WorkerName),
			},
			WorkerOutputLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
		)
	}
}

type requestCheckpointWorkerWaitStartTimeCtxKey struct{}

func getRequestCheckpointWorkerWaitStartTime(req tract.Request) *time.Time {
	creq, ok := req.(tract.ContextRequest)
	if !ok {
		return nil
	}

	ctx, done := creq.Context()
	start, _ := ctx.Value(requestCheckpointWorkerWaitStartTimeCtxKey{}).(time.Time)
	done(ctx)
	return &start
}

func setRequestCheckpointWorkerWaitStartTime(req tract.Request, start time.Time) {
	creq, ok := req.(tract.ContextRequest)
	if !ok {
		return
	}

	ctx, setCtx := creq.Context()
	setCtx(context.WithValue(ctx, requestCheckpointWorkerWaitStartTimeCtxKey{}, start))
}
