package tract

import (
	"context"
	"time"
)

// Request is the object that is passed along the tract.
// It keeps track of state by storing data via context values
type Request context.Context

// requestTimeStart is the key to retreive the generation time from a request.
type requestTimeStart struct{}

// GetRequestStartTime get the time the request was generated.
// If there is no start time, the zero value of time.Time is returned.
func GetRequestStartTime(r Request) time.Time {
	startTime, _ := r.Value(requestTimeStart{}).(time.Time)
	return startTime
}

func setRequestStartTime(r Request, t time.Time) Request {
	return context.WithValue(r, requestTimeStart{}, t)
}

type cleanup struct{}

// AddRequestCleanup add a function to the request that will be run when the request dies.
// This happens either when it reaches the end of a pool with no user set output, or a worker
// specified that the request should no longer continue.
func AddRequestCleanup(r Request, f func()) Request {
	if f == nil {
		return r
	}
	cleaupFuncs, _ := r.Value(cleanup{}).([]func())
	cleaupFuncs = append(cleaupFuncs, f)
	return context.WithValue(r, cleanup{}, cleaupFuncs)
}

func cleanupRequest(r Request) {
	cleanupFuncs, _ := r.Value(cleanup{}).([]func())
	for _, f := range cleanupFuncs {
		f()
	}
}
