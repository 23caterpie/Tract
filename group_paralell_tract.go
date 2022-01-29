package tract

import "fmt"

// NewParalellGroupTract makes a new tract that consists of muliple other tracts.
// Each request this tract receives is routed to 1 of its inner tracts.
// All requests proccessed by the inner tracts are routed to the same output.
//     ------------------
//     | / ( Tract0 ) \ |
//  -> | - ( Tract1 ) - | ->
//     | \ ( Tract2 ) / |
//     |     ...        |
//     ------------------
func NewParalellGroupTract[InputType, OutputType Request](
	name string,
	tracts ...Tract[InputType, OutputType],
) *ParalellGroupTract[InputType, OutputType] {
	return &ParalellGroupTract[InputType, OutputType]{
		name:   name,
		tracts: tracts,
	}
}

type ParalellGroupTract[InputType, OutputType Request] struct {
	name   string
	tracts []Tract[InputType, OutputType]
}

func (p *ParalellGroupTract[InputType, OutputType]) Name() string {
	return p.name
}

func (p *ParalellGroupTract[InputType, OutputType]) Init(
	input Input[RequestWrapper[InputType]],
	output Output[RequestWrapper[OutputType]],
) (TractStarter, error) {
	input, output = newOpencensusGroupLinks(p.name, input, output)
	starters := make([]TractStarter, len(p.tracts))
	for i := range p.tracts {
		var err error
		starters[i], err = p.tracts[i].Init(input, nonCloseOutput[RequestWrapper[OutputType]]{Output: output})
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
