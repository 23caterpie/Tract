package tract

// RegisterWorkerInputCheckpoint adds a checkpoint to the list of checkpoints to be run for all Inputs used in a worker tract.
// This is expected to be called in package init scripts to setup all tract workers the same way.
func RegisterWorkerInputCheckpoint(c InputCheckpoint[WorkerContext]) {
	registeredWorkerInputCheckpoints = append(registeredWorkerInputCheckpoints, c)
}

// RegisterGroupInputCheckpoint adds a checkpoint to the list of checkpoints to be run for all Inputs used in a group tract.
// This is expected to be called in package init scripts to setup all tract workers the same way.
func RegisterGroupInputCheckpoint(c InputCheckpoint[GroupContext]) {
	registeredGroupInputCheckpoints = append(registeredGroupInputCheckpoints, c)
}

type (
	// InputCheckpoint is a function that is called right before Input.Get() is called.
	InputCheckpoint[T any]    func(t T) InputCheckpointClosure
	initilizedInputCheckpoint func() InputCheckpointClosure
	// InputCheckpointClosure is a function that is called right after Input.Get() returns.
	InputCheckpointClosure func(Request, bool)
)

func initInputCheckpoints[T any](checkpoints []InputCheckpoint[T], t T) initilizedInputCheckpoint {
	return func() InputCheckpointClosure {
		checkpointCount := len(checkpoints)
		closures := make([]InputCheckpointClosure, checkpointCount)
		lastIndex := checkpointCount - 1
		for i, c := range checkpoints {
			// insert closures in reverse order to sim
			closures[lastIndex-i] = c(t)
		}
		return func(req Request, ok bool) {
			for _, c := range closures {
				c(req, ok)
			}
		}
	}
}

var (
	registeredWorkerInputCheckpoints []InputCheckpoint[WorkerContext]
	registeredGroupInputCheckpoints []InputCheckpoint[GroupContext]
)

func initRegisteredWorkerInputCheckpoints(workerContext WorkerContext) initilizedInputCheckpoint {
	return initInputCheckpoints(registeredWorkerInputCheckpoints, workerContext)
}

func initRegisteredGroupInputCheckpoints(groupContext GroupContext) initilizedInputCheckpoint {
	return initInputCheckpoints(registeredGroupInputCheckpoints, groupContext)
}

var _ Input[int64] = CheckpointInput[int64]{}

func NewCheckpointInput[T Request](
	checkpoint initilizedInputCheckpoint,
	base Input[T],
) Input[T] {
	return CheckpointInput[T]{
		checkpoint: checkpoint,
		base:       base,
	}
}

type CheckpointInput[T Request] struct {
	checkpoint initilizedInputCheckpoint
	base       Input[T]
}

func (w CheckpointInput[T]) Get() (T, bool) {
	closeCheckpoint := w.checkpoint()
	t, ok := w.base.Get()
	closeCheckpoint(t, ok)
	return t, ok
}
