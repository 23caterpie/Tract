package tract

import "errors"

// ErrFanOutAsHead is en error returned when a fanout group doesn't have its
// input set. Aka htere should be another Tract feeding into it.
var ErrFanOutAsHead = errors.New("fan out tract detected with no set input")

// ErrNoGroupMember is an error returned when a group tract doesn't have
// enough members.
var ErrNoGroupMember = errors.New("group tract detected with no inner tracts")

// chain chains multiple Tracts together.
// The result can collectively be viewed as a single larger tract.
//
//  ( Tract0 ) -> ( Tract1 ) -> ( Tract2 ) ...
func chain[T any](tracts ...Tract[T]) {
	lastTract := len(tracts) - 1
	for i := 0; i < lastTract; i++ {
		link(tracts[i], tracts[i+1])
	}
}

// link links 2 Tracts together.
//
// ( fromTract ) -> ( toTract )
func link[T any](from, to Tract[T]) {
	linkChannel := make(chan Request[T])
	from.SetOutput(OutputChannel[T](linkChannel))
	to.SetInput(InputChannel[T](linkChannel))
}

// NewSerialGroupTract makes a new tract that consists muliple other tracts.
// This accomplishes the same thing as chaining other tracts together manually,
// but has the benefit of being able to treat that chain of tracts as a single tract.
//     ----------------------------------------------
//  -> | ( Tract0 ) -> ( Tract1 ) -> ( Tract2 ) ... | ->
//     ----------------------------------------------
func NewSerialGroupTract[T any](name string, tract Tract[T], tracts ...Tract[T]) Tract[T] {
	tracts = append([]Tract[T]{tract}, tracts...)
	return &serialGroupTract[T]{
		name:   name,
		tracts: tracts,
	}
}

type serialGroupTract[T any] struct {
	name   string
	tracts []Tract[T]
}

func (p *serialGroupTract[_]) Name() string {
	return p.name
}

func (p *serialGroupTract[_]) Init() error {
	chain(p.tracts...)
	return p.init()
}

func (p *serialGroupTract[_]) init() error {
	if len(p.tracts) == 0 {
		return ErrNoGroupMember
	}
	var err error
	for _, tract := range p.tracts {
		err = tract.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *serialGroupTract[_]) Start() func() {
	callbacks := []func(){}
	for i := len(p.tracts) - 1; i >= 0; i-- {
		callbacks = append(callbacks, p.tracts[i].Start())
	}
	return func() {
		for i := len(callbacks) - 1; i >= 0; i-- {
			callbacks[i]()
		}
	}
}

func (p *serialGroupTract[T]) SetInput(in Input[T]) {
	if len(p.tracts) == 0 {
		return
	}
	p.tracts[0].SetInput(in)
}

func (p *serialGroupTract[T]) SetOutput(out Output[T]) {
	if len(p.tracts) == 0 {
		return
	}
	p.tracts[len(p.tracts)-1].SetOutput(out)
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
func NewParalellGroupTract[T any](name string, tract Tract[T], tracts ...Tract[T]) Tract[T] {
	tracts = append([]Tract[T]{tract}, tracts...)
	pTract := &paralellGroupTract[T]{}
	pTract.name = name
	pTract.tracts = tracts
	pTract.output = FinalOutput[T]{}
	return pTract
}

type paralellGroupTract[T any] struct {
	serialGroupTract[T]
	output Output[T]
}

func (p *paralellGroupTract[_]) Init() error {
	return p.init()
}

func (p *paralellGroupTract[_]) Start() func() {
	wait := p.serialGroupTract.Start()
	return func() {
		wait()
		p.output.Close()
	}
}

func (p *paralellGroupTract[T]) SetInput(in Input[T]) {
	if len(p.tracts) == 0 {
		return
	}
	for _, tract := range p.tracts {
		tract.SetInput(in)
	}
}

func (p *paralellGroupTract[T]) SetOutput(out Output[T]) {
	if len(p.tracts) == 0 {
		return
	}
	for _, tract := range p.tracts {
		tract.SetOutput(nonCloseOutput[T]{Output: out})
	}
	p.output = out
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
func NewFanOutGroupTract[T any](name string, tract Tract[T], tracts ...Tract[T]) Tract[T] {
	tracts = append([]Tract[T]{tract}, tracts...)
	fanOutTract := &fanOutTract[T]{
		input: InputGenerator[T]{},
	}
	fTract := &fanOutGroupTract[T]{}
	fTract.name = name
	fTract.tracts = append(
		[]Tract[T]{fanOutTract},
		tracts...,
	)
	fTract.output = FinalOutput[T]{}
	return fTract
}

type fanOutGroupTract[T any] struct {
	serialGroupTract[T]
	output Output[T]
}

func (p *fanOutGroupTract[T]) Init() error {
	if _, weAreHeadTract := p.tracts[0].(*fanOutTract[T]).input.(InputGenerator[T]); weAreHeadTract {
		return ErrFanOutAsHead
	}
	if len(p.tracts) <= 1 {
		return ErrNoGroupMember
	}
	// Connect the fan out tract to all the other tracts.
	for _, tract := range p.tracts[1:] {
		link(p.tracts[0], tract)
	}
	return p.init()
}

func (p *fanOutGroupTract[_]) Start() func() {
	wait := p.serialGroupTract.Start()
	return func() {
		wait()
		p.output.Close()
	}
}

func (p *fanOutGroupTract[T]) SetOutput(out Output[T]) {
	// The first tract is always the fanOutTract, which should not be included in the true list of inner tracts the user knows/cares about.
	if len(p.tracts) <= 1 {
		return
	}
	for _, tract := range p.tracts[1:] {
		tract.SetOutput(nonCloseOutput[T]{Output: out})
	}
	p.output = out
}
