package octract

import "fmt"

const (
	TractTypeGroup  = `group`
	TractTypeWorker = `worker`
)

// TODO: consider attaching spans on the request context on output and end it on get. Can be used for stats too.
const (
	CheckpointTypeWork = `work`
)

func makeCheckpointSpanName(tractType, tractName, checkpointType string) string {
	return fmt.Sprintf(`octract/%s/%s/checkpoint/%s`, tractType, tractName, checkpointType)
}
