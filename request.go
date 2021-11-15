package tract

import (
	"context"
	"time"
)

// Request is the object that is passed along the tract.
// It keeps track of state by storing data via context values
type Request[T any] struct {
	ctx  context.Context
	Data T
}

func (r Request[_]) Context() context.Context {
	if r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

func (r *Request[_]) WithContext(ctx context.Context) {
	if ctx == nil {
		panic("nil context")
	}
	r.ctx = ctx
}

// requestTimeStart is the key to retreive the generation time from a request.
// Request value type is time.Time
type requestTimeStartKey struct{}

// GetRequestStartTime get the time the request was generated.
// If there is no start time, the zero value of time.Time is returned.
func GetRequestStartTime[T any](r Request[T]) time.Time {
	startTime, _ := r.Context().Value(requestTimeStartKey{}).(time.Time)
	return startTime
}

func setRequestStartTime[T any](r *Request[T], t time.Time) {
	r.WithContext(context.WithValue(r.Context(), requestTimeStartKey{}, t))
}

// Request value type is cleanups
type cleanupKey struct{}
type cleanups[T any] []func(r Request[T], success bool)

// AddRequestCleanup add a function to the request that will be run when the request dies.
// This happens either when it reaches the end of a pool with no user set output, or a worker
// specified that the request should no longer continue.
func AddRequestCleanup[T any](r *Request[T], f func(Request[T], bool)) {
	if f == nil {
		return
	}
	ctx := r.Context()
	cleanupFuncs, _ := ctx.Value(cleanupKey{}).(cleanups[T])
	cleanupFuncs = append(cleanupFuncs, f)
	r.WithContext(context.WithValue(ctx, cleanupKey{}, cleanupFuncs))
}

// RemoveAllRequestCleanups removes all of the cleanups attached to the request.
// This does not run the cleanups.
func RemoveAllRequestCleanups[T any](r *Request[T]) {
	r.WithContext(context.WithValue(r.Context(), cleanupKey{}, nil))
}

// CleanupRequest manually calls all the cleanup functions attached to the request.
// This does not remove the cleanups.
func CleanupRequest[T any](r Request[T], success bool) {
	cleanupRequest(r, success)
}

func cleanupRequest[T any](r Request[T], success bool) {
	cleanupFuncs, _ := r.Context().Value(cleanupKey{}).(cleanups[T])
	for _, f := range cleanupFuncs {
		f(r, success)
	}
}

// swapCleanups sets the request cleanup ot be the provided cleanup, and retunrs the old cleanup.
func swapCleanups[T any](r *Request[T], cleanupFuncs cleanups[T]) cleanups[T] {
	ctx := r.Context()
	oldCleaupFuncs, _ := ctx.Value(cleanupKey{}).(cleanups[T])
	r.WithContext(context.WithValue(ctx, cleanupKey{}, cleanupFuncs))
	return oldCleaupFuncs
}
