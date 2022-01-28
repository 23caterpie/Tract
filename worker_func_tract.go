package tract

import "context"

// NewWorkerFuncTract returns a Worker Tract using the function provided as the worker.
// ex: sql.(*DB).PrepareContext
func NewWorkerFuncTract[InputType, OutputType Request](
	name string,
	size int,
	f WorkerFunc[InputType, OutputType],
) Tract[InputType, OutputType] {
	return NewWorkerFactoryTract(name, size, NewFactoryFromWorker(NewWorkerFunc(f)))
}

func NewWorkerFunc[InputType, OutputType Request](
	f WorkerFunc[InputType, OutputType],
) Worker[InputType, OutputType] {
	return f
}

type WorkerFunc[InputType, OutputType Request] func(context.Context, InputType) (OutputType, error)

func (f WorkerFunc[InputType, OutputType]) Work(ctx context.Context, input InputType) (OutputType, error) {
	return f(ctx, input)
}

// NewBasicWorkerFuncTract returns a Worker Tract using the function provided as the worker.
// ex: math.Sqrt
func NewBasicWorkerFuncTract[InputType, OutputType Request](
	name string,
	size int,
	f BasicWorkerFunc[InputType, OutputType],
) Tract[InputType, OutputType] {
	return NewWorkerFactoryTract(name, size, NewFactoryFromWorker(NewBasicWorkerFunc(f)))
}

func NewBasicWorkerFunc[InputType, OutputType Request](
	f BasicWorkerFunc[InputType, OutputType],
) Worker[InputType, OutputType] {
	return f
}

type BasicWorkerFunc[InputType, OutputType Request] func(InputType) OutputType

func (f BasicWorkerFunc[InputType, OutputType]) Work(_ context.Context, input InputType) (OutputType, error) {
	return f(input), nil
}

// NewErrorWorkerFuncTract returns a Worker Tract using the function provided as the worker.
// ex: ioutil.Discard.Write
func NewErrorWorkerFuncTract[InputType, OutputType Request](
	name string,
	size int,
	f ErrorWorkerFunc[InputType, OutputType],
) Tract[InputType, OutputType] {
	return NewWorkerFactoryTract(name, size, NewFactoryFromWorker(NewErrorWorkerFunc(f)))
}

func NewErrorWorkerFunc[InputType, OutputType Request](
	f ErrorWorkerFunc[InputType, OutputType],
) Worker[InputType, OutputType] {
	return f
}

type ErrorWorkerFunc[InputType, OutputType Request] func(InputType) (OutputType, error)

func (f ErrorWorkerFunc[InputType, OutputType]) Work(_ context.Context, input InputType) (OutputType, error) {
	return f(input)
}
