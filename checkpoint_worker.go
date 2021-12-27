package tract

// RegisterWorkerWorkCheckpoint adds a checkpoint to the list of checkpoints to be run for all Workers used in a worker tract.
// This is expected to be called in package init scripts to setup all tract workers the same way.
func RegisterWorkerWorkCheckpoint(c WorkerWorkCheckpoint[WorkerContext]) {
	registeredWorkerWorkCheckpoints = append(registeredWorkerWorkCheckpoints, c)
}

type (
	// WorkerWorkCheckpoint is a function that is called right before Worker.Work() is called.
	WorkerWorkCheckpoint[T any]    func(t T, inputRequest Request) WorkerWorkCheckpointClosure
	initilizedWorkerWorkCheckpoint func(inputRequest Request) WorkerWorkCheckpointClosure
	// WorkerWorkCheckpointClosure is a function that is called right after Worker.Work() returns.
	WorkerWorkCheckpointClosure func(outputRequest Request)
)

func initWorkerCheckpoints[T any](checkpoints []WorkerWorkCheckpoint[T], t T) initilizedWorkerWorkCheckpoint {
	return func(req Request) WorkerWorkCheckpointClosure {
		checkpointCount := len(checkpoints)
		closures := make([]WorkerWorkCheckpointClosure, checkpointCount)
		lastIndex := checkpointCount - 1
		for i, c := range checkpoints {
			// insert closures in reverse order to sim
			closures[lastIndex-i] = c(t, req)
		}
		return func(req Request) {
			for _, c := range closures {
				c(req)
			}
		}
	}
}

var (
	registeredWorkerWorkCheckpoints []WorkerWorkCheckpoint[WorkerContext]
)

func initRegisteredWorkerWorkCheckpoints(workerContext WorkerContext) initilizedWorkerWorkCheckpoint {
	return initWorkerCheckpoints(registeredWorkerWorkCheckpoints, workerContext)
}

var _ Worker[int64, float64] = CheckpointWorker[int64, float64]{}

func NewCheckpointWorker[InputType, OutputType Request](
	checkpoint initilizedWorkerWorkCheckpoint,
	base Worker[InputType, OutputType],
) Worker[InputType, OutputType] {
	return CheckpointWorker[InputType, OutputType]{
		checkpoint: checkpoint,
		base:       base,
	}
}

type CheckpointWorker[InputType, OutputType Request] struct {
	checkpoint initilizedWorkerWorkCheckpoint
	base       Worker[InputType, OutputType]
}

func (w CheckpointWorker[InputType, OutputType]) Work(input InputType) (OutputType, bool) {
	closeCheckpoint := w.checkpoint(input)
	output, ok := w.base.Work(input)
	closeCheckpoint(output)
	return output, ok
}
