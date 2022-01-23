package tract

import "time"

type opencensusData struct {
	// workerData is information about worker tracts used for metrics/tracing.
	// This data is set on the input to a worker tract and used on the output of
	// that worker tract.
	workerData opencensusUnitData
	// groupDataStack is information about group tracts used for metrics/tracing.
	// This data is pushed on the stack on the input to a group tract
	// and popped on the output of that group tract.
	// This is a stack since groups can contain more groups that will finish before
	// the parent group finishes.
	// TODO: can maybe merge workerDataStack and groupDataStack?
	groupDataStack []opencensusUnitData
}

type opencensusUnitData struct {
	name    string
	start   time.Time
	endSpan func()
}

func (d opencensusData) clone() opencensusData {
	groupDataStack := make([]opencensusUnitData, len(d.groupDataStack))
	copy(groupDataStack, d.groupDataStack)
	return opencensusData{
		workerData:     d.workerData,
		groupDataStack: groupDataStack,
	}
}
