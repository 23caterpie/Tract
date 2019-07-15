package tract

import (
	"context"
	"time"
)

// Request is the object that is passed along the tract.
// It keeps track of state by storing data via context values
type Request context.Context

// requestTimeStart is the key to retreive the generation time from a request.
// Request value type is time.Time
type requestTimeStartKey struct{}

// GetRequestStartTime get the time the request was generated.
// If there is no start time, the zero value of time.Time is returned.
func GetRequestStartTime(r Request) time.Time {
	startTime, _ := r.Value(requestTimeStartKey{}).(time.Time)
	return startTime
}

func setRequestStartTime(r Request, t time.Time) Request {
	return context.WithValue(r, requestTimeStartKey{}, t)
}

// Request value type is cleanups
type cleanupKey struct{}
type cleanups []func(r Request, success bool)

// AddRequestCleanup add a function to the request that will be run when the request dies.
// This happens either when it reaches the end of a pool with no user set output, or a worker
// specified that the request should no longer continue.
func AddRequestCleanup(r Request, f func(Request, bool)) Request {
	if f == nil {
		return r
	}
	cleanupFuncs, _ := r.Value(cleanupKey{}).(cleanups)
	cleanupFuncs = append(cleanupFuncs, f)
	return context.WithValue(r, cleanupKey{}, cleanupFuncs)
}

// RemoveAllRequestCleanups removes all of the cleanups attached to the request.
// This does not run the cleanups.
func RemoveAllRequestCleanups(r Request) Request {
	return context.WithValue(r, cleanupKey{}, nil)
}

// CleanupRequest manually calls all the cleanup functions attached to the request.
// This does not remove the cleanups.
func CleanupRequest(r Request, success bool) {
	cleanupRequest(r, success)
}

func cleanupRequest(r Request, success bool) {
	cleanupFuncs, _ := r.Value(cleanupKey{}).(cleanups)
	for _, f := range cleanupFuncs {
		f(r, success)
	}
}

// swapCleanups sets the request cleanup ot be the provided cleanup, and retunrs the old cleanup.
func swapCleanups(r Request, cleanupFuncs cleanups) (Request, cleanups) {
	oldCleaupFuncs, _ := r.Value(cleanupKey{}).(cleanups)
	return context.WithValue(r, cleanupKey{}, cleanupFuncs), oldCleaupFuncs
}
