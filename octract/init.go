package octract

import tract "github.com/23caterpie/Tract"

func init() {
	tract.RegisterWorkerWorkCheckpoint(workerWorkCheckpoint)
	tract.RegisterWorkerInputCheckpoint(workerInputCheckpoint)
	tract.RegisterWorkerOutputCheckpoint(workerOutputCheckpoint)

	tract.RegisterGroupInputCheckpoint(groupInputCheckpoint)
	tract.RegisterGroupOutputCheckpoint(groupOutputCheckpoint)
}
