package tract

import "fmt"

// NewSerialGroupTract makes a new tract that consists muliple other tracts.
// This accomplishes the same thing as chaining other tracts together manually,
// but has the benefit of being able to treat that chain of tracts as a single tract.
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

func NewNamedLinker[InputType, InnerType, OutputType Request](
	name string,
	tract Tract[InputType, InnerType],
) Linker[InputType, InnerType, OutputType] {
	return Linker[InputType, InnerType, OutputType]{
		name: name,
		head: tract,
	}
}

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
