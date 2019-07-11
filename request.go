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
