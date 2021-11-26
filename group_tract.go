package tract

import "fmt"

// NewSerialGroupTract makes a new tract that consists muliple other tracts.
// This accomplishes the same thing as chaining other tracts together manually,
// but has the benefit of being able to treat that chain of tracts as a single tract.
//     ----------------------------------------------
//  -> | ( Tract0 ) -> ( Tract1 ) -> ( Tract2 ) ... | ->
//     ----------------------------------------------
func NewSerialGroupTract[InputType, InnerType, OutputType any](
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

type SerialGroupTract[InputType, InnerType, OutputType any] struct {
	name string
	head Tract[InputType, InnerType]
	tail Tract[InnerType, OutputType]
}

func (p *SerialGroupTract[InputType, InnerType, OutputType]) Name() string {
	return p.name
}

func (p *SerialGroupTract[InputType, InnerType, OutputType]) Init(
	input Input[InputType],
	output Output[OutputType],
) (TractStarter, error) {
	link := Channel[InnerType](make(chan InnerType))

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

func ContinueSerialGroupTract[InputType, InputInnerType, OutputInnerType, OutputType any](
	head *SerialGroupTract[InputType, InputInnerType, OutputInnerType],
	tail Tract[OutputInnerType, OutputType],
) *SerialGroupTract[InputType, OutputInnerType, OutputType] {
	return &SerialGroupTract[InputType, OutputInnerType, OutputType]{
		name: head.name,
		head: head,
		tail: tail,
	}
}

// NewParalellGroupTract makes a new tract that consists of muliple other tracts.
// Each request this tract receives is routed to 1 of its inner tracts.
// All requests proccessed by the inner tracts are routed to the same output.
//     ------------------
//     | / ( Tract0 ) \ |
//  -> | - ( Tract1 ) - | ->
//     | \ ( Tract2 ) / |
//     |     ...        |
//     ------------------
func NewParalellGroupTract[InputType, OutputType any](
	name string,
	tracts ...Tract[InputType, OutputType],
) *ParalellGroupTract[InputType, OutputType] {
	return &ParalellGroupTract[InputType, OutputType]{
		name:   name,
		tracts: tracts,
	}
}

type ParalellGroupTract[InputType, OutputType any] struct {
	name   string
	tracts []Tract[InputType, OutputType]
}

func (p *ParalellGroupTract[InputType, OutputType]) Name() string {
	return p.name
}

func (p *ParalellGroupTract[InputType, OutputType]) Init(
	input Input[InputType],
	output Output[OutputType],
) (TractStarter, error) {
	starters := make([]TractStarter, len(p.tracts))
	for i := range p.tracts {
		var err error
		starters[i], err = p.tracts[i].Init(input, nonCloseOutput[OutputType]{Output: output})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize tract[%d] %q: %w", i, p.tracts[i].Name(), err)
		}
	}

	return tractStarterFunc(func() TractWaiter {
		waiters := make([]TractWaiter, len(starters))
		for i := range starters {
			waiters[i] = starters[i].Start()
		}
		return tractWaiterFunc(func() {
			for i := range waiters {
				waiters[i].Wait()
			}
			if output != nil {
				output.Close()
			}
		})
	}), nil
}

// NewFanOutGroupTract makes a new tract that consists muliple other tracts.
// Each request this tract receives is routed to all of its inner tracts.
// All requests proccessed by the inner tracts are routed to the same output.
// This Tract should not be the first tract in a group as it has no machanism
// of closing on it's own. Aka it's input must be set to something.
//     ------------------
//     | / ( Tract0 ) \ |
//  -> | - ( Tract1 ) - | ->
//     | \ ( Tract2 ) / |
//     |     ...        |
//     ------------------
func NewFanOutGroupTract[InputType, InnerType, OutputType any](
	name string,
	tract Tract[InputType, InnerType],
	tracts ...Tract[InnerType, OutputType],
) *FanOutGroupTract[InputType, InnerType, OutputType] {
	return &FanOutGroupTract[InputType, InnerType, OutputType]{
		name:  name,
		head:  tract,
		tails: tracts,
	}
}

type FanOutGroupTract[InputType, InnerType, OutputType any] struct {
	name  string
	head  Tract[InputType, InnerType]
	tails []Tract[InnerType, OutputType]
}

func (p *FanOutGroupTract[InputType, InnerType, OutputType]) Name() string {
	return p.name
}

func (p *FanOutGroupTract[InputType, InnerType, OutputType]) Init(
	input Input[InputType],
	output Output[OutputType],
) (TractStarter, error) {
	links := make([]Channel[InnerType], len(p.tails))
	for i := range links {
		links[i] = make(chan InnerType)
	}

	headerStarter, err := p.head.Init(input, outputs[InnerType, Channel[InnerType]](links))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize head tract %q: %w", p.head.Name(), err)
	}

	tailStarters := make([]TractStarter, len(p.tails))
	for i := range p.tails {
		var err error
		tailStarters[i], err = p.tails[i].Init(links[i], nonCloseOutput[OutputType]{Output: output})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize tail tract[%d] %q: %w", i, p.tails[i].Name(), err)
		}
	}

	return tractStarterFunc(func() TractWaiter {
		tailWaiters := make([]TractWaiter, len(tailStarters))
		for i := range tailStarters {
			tailWaiters[i] = tailStarters[i].Start()
		}
		headWaiter := headerStarter.Start()
		return tractWaiterFunc(func() {
			headWaiter.Wait()
			for i := range tailWaiters {
				tailWaiters[i].Wait()
			}
			if output != nil {
				output.Close()
			}
		})
	}), nil
}
