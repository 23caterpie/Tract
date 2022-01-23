package tract

// NewTractRunner provides a simplified interface for runner a tract with a given input and output.
func NewTractRunner[InputType, OutputType Request](
	input Input[InputType],
	tract Tract[InputType, OutputType],
	output Output[OutputType],
) *TractRunner[InputType, OutputType] {
	return &TractRunner[InputType, OutputType]{
		input:  input,
		tract:  tract,
		output: output,
	}
}

type TractRunner[InputType, OutputType Request] struct {
	input  Input[InputType]
	tract  Tract[InputType, OutputType]
	output Output[OutputType]

	WithBaseContext BaseContext[InputType]
}

// Name returns the name of the tract.
func (t *TractRunner[InputType, OutputType]) Name() string {
	return t.tract.Name()
}

// Run runs the tract according to the documented usage of a tract using the runner's input and output.
func (t *TractRunner[InputType, OutputType]) Run() error {
	inputWrapper := NewRequestWrapperInput(t.input)
	if t.WithBaseContext != nil {
		inputWrapper.BaseContext = t.WithBaseContext
	}
	starter, err := t.tract.Init(
		inputWrapper,
		NewRequestWrapperOutput(t.output),
	)
	if err != nil {
		return err
	}
	starter.Start().Wait()
	return nil
}
