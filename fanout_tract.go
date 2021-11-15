package tract

import "sync"

type fanOutTract[T any] struct {
	input   Input[T]
	outputs []Output[T]
}

func (p *fanOutTract[_]) Name() string {
	return "fanout"
}

func (p *fanOutTract[_]) Init() error {
	return nil
}

func (p *fanOutTract[_]) Start() func() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			inputValue, ok := p.input.Get()
			if !ok {
				break
			}
			for _, output := range p.outputs {
				output.Put(inputValue)
			}
		}
	}()

	return func() {
		wg.Wait()
		for _, output := range p.outputs {
			output.Close()
		}
	}
}

func (p *fanOutTract[T]) SetInput(in Input[T]) {
	p.input = in
}

// fanOutTract is never available directly externally. It's outputs are only set internally,
// so this implementation of ever growing number of outputs is not liable to growing out of control.
func (p *fanOutTract[T]) SetOutput(out Output[T]) {
	p.outputs = append(p.outputs, out)
}
