package tract

// RegisterWorkerOutputCheckpoint adds a checkpoint to the list of checkpoints to be run for all Outputs used in a worker tract.
// This is expected to be called in package init scripts to setup all tract workers the same way.
func RegisterWorkerOutputCheckpoint(c OutputCheckpoint[WorkerContext]) {
	registeredWorkerOutputCheckpoints = append(registeredWorkerOutputCheckpoints, c)
}

// RegisterGroupOutputCheckpoint adds a checkpoint to the list of checkpoints to be run for all Outputs used in a group tract.
// This is expected to be called in package init scripts to setup all tract workers the same way.
func RegisterGroupOutputCheckpoint(c OutputCheckpoint[GroupContext]) {
	registeredGroupOutputCheckpoints = append(registeredGroupOutputCheckpoints, c)
}

type (
	// OutputCheckpoint is a function that is called right before Output.Put() is called.
	OutputCheckpoint[T any]    func(t T, req Request) OutputCheckpointClosure
	initilizedOutputCheckpoint func(Request) OutputCheckpointClosure
	// OutputCheckpointClosure is a function that is called right after Output.Put() returns.
	// Note that the request object should not be mutated from inside this function since it can race since that request has already been put
	OutputCheckpointClosure func()
)

func initOutputCheckpoints[T any](checkpoints []OutputCheckpoint[T], t T) initilizedOutputCheckpoint {
	return func(req Request) OutputCheckpointClosure {
		checkpointCount := len(checkpoints)
		closures := make([]OutputCheckpointClosure, checkpointCount)
		lastIndex := checkpointCount - 1
		for i, c := range checkpoints {
			// insert closures in reverse order to sim
			closures[lastIndex-i] = c(t, req)
		}
		return func() {
			for _, c := range closures {
				c()
			}
		}
	}
}

var (
	registeredWorkerOutputCheckpoints []OutputCheckpoint[WorkerContext]
	registeredGroupOutputCheckpoints []OutputCheckpoint[GroupContext]
)

func initRegisteredWorkerOutputCheckpoints(workerContext WorkerContext) initilizedOutputCheckpoint {
	return initOutputCheckpoints(registeredWorkerOutputCheckpoints, workerContext)
}

func initRegisteredGroupOutputCheckpoints(groupContext GroupContext) initilizedOutputCheckpoint {
	return initOutputCheckpoints(registeredGroupOutputCheckpoints, groupContext)
}

var _ Output[int64] = CheckpointOutput[int64]{}

func NewCheckpointOutput[T Request](
	checkpoint initilizedOutputCheckpoint,
	base Output[T],
) Output[T] {
	return CheckpointOutput[T]{
		checkpoint: checkpoint,
		base:       base,
	}
}

type CheckpointOutput[T Request] struct {
	checkpoint initilizedOutputCheckpoint
	base       Output[T]
}

func (w CheckpointOutput[T]) Put(t T) {
	closeCheckpoint := w.checkpoint(t)
	w.base.Put(t)
	closeCheckpoint()
}

func (w CheckpointOutput[T]) Close() {
	w.base.Close()
}
