package tract

import "fmt"

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
func NewFanOutGroupTract[InputType, InnerType, OutputType Request](
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

type FanOutGroupTract[InputType, InnerType, OutputType Request] struct {
	name  string
	head  Tract[InputType, InnerType]
	tails []Tract[InnerType, OutputType]
}

func (p *FanOutGroupTract[InputType, InnerType, OutputType]) Name() string {
	return p.name
}

func (p *FanOutGroupTract[InputType, InnerType, OutputType]) Init(
	input Input[RequestWrapper[InputType]],
	output Output[RequestWrapper[OutputType]],
) (TractStarter, error) {
	input, output = newOpencensusGroupLinks(p.name, input, output)
	links := make([]Channel[RequestWrapper[InnerType]], len(p.tails))
	for i := range links {
		links[i] = make(chan RequestWrapper[InnerType])
	}

	headerStarter, err := p.head.Init(input, outputs[InnerType, Channel[RequestWrapper[InnerType]]](links))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize head tract %q: %w", p.head.Name(), err)
	}

	tailStarters := make([]TractStarter, len(p.tails))
	for i := range p.tails {
		var err error
		tailStarters[i], err = p.tails[i].Init(links[i], nonCloseOutput[RequestWrapper[OutputType]]{Output: output})
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
