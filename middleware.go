package tract

// type Middleware[T, D any] func(T, D) D

// func ChainMiddleware[T, D any](t T, d D, ms ...Middleware[T, D]) D {
// 	for _, m := range ms {
// 		d = m(t, d)
// 	}
// 	return d
// }

// type WorkerContext struct {
// 	WorkerName string
// }

// // type WorkerMiddleware[InputType, OutputType Request] Middleware[string, Worker[InputType, OutputType]]
// type WorkerMiddleware Middleware[WorkerContext, Worker[Request, Request]]
// type InputMiddleware Middleware[WorkerContext, Input[Request]]
// type OutputMiddleware Middleware[WorkerContext, Output[Request]]

// // RegisterWorkerMiddleware adds a middleware to the list of middlewares to be used for all Workers used in a worker tract.
// // This is expected to be called in package init scripts to setup all tract workers the same way.
// func RegisterWorkerMiddleware(m WorkerMiddleware) {
// 	registeredWorkerMiddlewares = append(registeredWorkerMiddlewares, Middleware[WorkerContext, Worker[Request, Request]](m))
// }

// var (
// 	registeredWorkerMiddlewares []Middleware[WorkerContext, Worker[Request, Request]]
// )

// // func x() {
// // 	var (
// // 		ys []WorkerMiddleware
// // 		z Worker[int64, float64]
// // 	)

// // 	// TODO: this requirement is weeeeird.
// // 	var yys []Middleware[string, Worker[Request, Request]]
// // 	for _, y := range ys {
// // 		yys = append(yys, Middleware[string, Worker[Request, Request]](y))
// // 	}

// // 	_ = wrapWorker(
// // 		"xxx",
// // 		z,
// // 		yys...,
// // 	)
// // }

// func wrapRegisteredWorker[InputType, OutputType Request](
// 	workerName string,
// 	worker Worker[InputType, OutputType],
// ) Worker[InputType, OutputType] {
// 	return wrapWorker(
// 		workerName,
// 		worker,
// 		registeredWorkerMiddlewares...,
// 	)
// }

// func wrapWorker[InputType, OutputType Request](
// 	workerName string,
// 	worker Worker[InputType, OutputType],
// 	wrappers ...Middleware[WorkerContext, Worker[Request, Request]],
// ) Worker[InputType, OutputType] {
// 	return wrapperWorker[InputType, OutputType]{
// 		Worker: ChainMiddleware[WorkerContext, Worker[Request, Request]](
// 			WorkerContext{
// 				WorkerName: workerName,
// 			},
// 			requestWorker[InputType, OutputType]{Worker: worker},
// 			wrappers...,
// 		),
// 	}
// }

// var _ Worker[int, float64] = wrapperWorker[int, float64]{}

// type wrapperWorker[InputType, OutputType Request] struct {
// 	Worker[Request, Request]
// }

// func (w wrapperWorker[InputType, OutputType]) Work(input InputType) (OutputType, bool) {
// 	output, ok := w.Worker.Work(input)
// 	return output.(OutputType), ok
// }

// var _ Worker[Request, Request] = requestWorker[int, float64]{}

// type requestWorker[InputType, OutputType Request] struct {
// 	Worker[InputType, OutputType]
// }

// func (w requestWorker[InputType, OutputType]) Work(input Request) (Request, bool) {
// 	return w.Worker.Work(input.(InputType))
// }
