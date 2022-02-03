package tract

import "fmt"

// NewSerialGroupTract makes a new tract that consists muliple other tracts.
// This has the benefit of being able to treat many tracts as a single tract.
//     ----------------------------------------------
//  -> | ( Tract0 ) -> ( Tract1 ) -> ( Tract2 ) ... | ->
//     ----------------------------------------------
func NewSerialGroupTract[InputType, InnerType, OutputType Request](
	name string,
	head Tract[InputType, InnerType],
	tail Tract[InnerType, OutputType],
) *SerialGroupTract[InputType, InnerType, OutputType] {
	return &SerialGroupTract[InputType, InnerType, OutputType]{
		name: name,
		head: head,
		tail: tail,
	}
}

type SerialGroupTract[InputType, InnerType, OutputType Request] struct {
	name string
	head Tract[InputType, InnerType]
	tail Tract[InnerType, OutputType]
}

func (p *SerialGroupTract[InputType, InnerType, OutputType]) Name() string {
	return p.name
}

func (p *SerialGroupTract[InputType, InnerType, OutputType]) Init(
	input Input[RequestWrapper[InputType]],
	output Output[RequestWrapper[OutputType]],
) (TractStarter, error) {
	input, output = newOpencensusGroupLinks(p.name, input, output)
	link := Channel[RequestWrapper[InnerType]](make(chan RequestWrapper[InnerType]))

	headerStarter, err := p.head.Init(input, link)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize head tract %q: %w", p.head.Name(), err)
	}
	tailStarter, err := p.tail.Init(link, output)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tail tract %q: %w", p.tail.Name(), err)
	}
	return tractStarterFunc(func() TractWaiter {
		tailWaiter := tailStarter.Start()
		headWaiter := headerStarter.Start()
		return tractWaiterFunc(func() {
			headWaiter.Wait()
			tailWaiter.Wait()
		})
	}), nil
}

// NewNamedLinker is a facilitator for SerialGroupTract.
// This allows you to link many tracts together in a semi-non-nested way despite generic methods not being allowed.
// usage:
// 		myTract := tract.NewNamedLinker[T, T, T](
// 			"myGroup",
// 			tract.NewWorkerTract("myWorker1", 1, myWorker1),
// 		).Link(tract.NewLinker[T, T, T](
// 			tract.NewWorkerTract("myWorker2", 1, myWorker2),
// 		).Link(tract.NewLinker[T, T, T](
// 			tract.NewWorkerTract("myWorker3", 1, myWorker3),
// 		).Link(tract.NewLinker[T, T, T](
// 			tract.NewWorkerTract("myWorker4", 1, myWorker4),
// 		).Link(tract.NewLinker[T, T, T](
// 			tract.NewWorkerTract("myWorker5", 1, myWorker5),
// 		).Link(tract.NewLinker[T, T, T](
// 			tract.NewWorkerTract("myWorker6", 1, myWorker6),
// 		...
// 		).Link(tract.NewLinker[T, T, T](
// 			tract.NewWorkerTract("myWorkerN-1", 1, myWorkerN_1),
// 		).Link(
// 			tract.NewWorkerTract("myWorkerN", 1, myWorkerN),
// 		)))))))
// 		...
func NewNamedLinker[InputType, InnerType, OutputType Request](
	name string,
	tract Tract[InputType, InnerType],
) Linker[InputType, InnerType, OutputType] {
	return Linker[InputType, InnerType, OutputType]{
		name: name,
		head: tract,
	}
}

// NewLinker is the same as NewNamedLinker without a name.
// Particularly useful if this is defining a part of a named parent tract.
// Non named tracts do not make metrics/traces.
func NewLinker[InputType, InnerType, OutputType Request](
	tract Tract[InputType, InnerType],
) Linker[InputType, InnerType, OutputType] {
	return Linker[InputType, InnerType, OutputType]{
		head: tract,
	}
}

type Linker[InputType, InnerType, OutputType Request] struct {
	name string
	head Tract[InputType, InnerType]
}

func (l Linker[InputType, InnerType, OutputType]) Link(
	tail Tract[InnerType, OutputType],
) Tract[InputType, OutputType] {
	return NewSerialGroupTract(l.name, l.head, tail)
}
