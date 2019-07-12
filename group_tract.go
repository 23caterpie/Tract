package tract

// Chain chains multiple Tracts together.
// The result can collectively be viewed as a single larger tract.
//
// ( Tract0 ) -> ( Tract1 ) -> ( Tract2 ) ...
func Chain(tracts ...Tract) {
	lastTract := len(tracts) - 1
	for i := 0; i < lastTract; i++ {
		link(tracts[i], tracts[i+1])
	}
}

// link links 2 Tracts together.
//
// ( fromTract ) -> ( toTract )
func link(from, to Tract) {
	link := make(chan Request)
	from.SetOutput(OutputChannel(link))
	to.SetInput(InputChannel(link))
}

// NewSerialGroupTract makes a new tract that consists muliple other tracts.
// This accomplishes the same thing as chaining other tracts together manually,
// but has the benefit of being able to treat that chain of tracts as a single tract.
//    -------------------------------------------
// -> | ( Tract0 ) -> ( Tract1 ) -> ( Tract2 ) ... | ->
//    -------------------------------------------
func NewSerialGroupTract(name string, tracts ...Tract) Tract {
	Chain(tracts...)
	return &serialGroupTract{
		name:   name,
		tracts: tracts,
	}
}

type serialGroupTract struct {
	name   string
	tracts []Tract
}

func (p *serialGroupTract) Name() string {
	return p.name
}

func (p *serialGroupTract) Init() error {
	var err error
	for _, tract := range p.tracts {
		err = tract.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *serialGroupTract) Start() func() {
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

func (p *serialGroupTract) SetInput(in Input) {
	if len(p.tracts) == 0 {
		return
	}
	p.tracts[0].SetInput(in)
}

func (p *serialGroupTract) SetOutput(out Output) {
	if len(p.tracts) == 0 {
		return
	}
	p.tracts[len(p.tracts)-1].SetOutput(out)
}

// NewParalellGroupTract makes a new tract that consists muliple other tracts.
// Each request this tract receives is routed to 1 of its inner tracts.
// All requests proccessed by the inner tracts are routed to the same output.
//    -----------------
//    | / ( Tract0 ) \ |
// -> | - ( Tract1 ) - | ->
//    | \ ( Tract2 ) / |
//    |      ...      |
//    -----------------
func NewParalellGroupTract(name string, tracts ...Tract) Tract {
	tract := &paralellGroupTract{}
	tract.name = name
	tract.tracts = tracts
	return tract
}

type paralellGroupTract struct {
	serialGroupTract
}

func (p *paralellGroupTract) SetInput(in Input) {
	if len(p.tracts) == 0 {
		return
	}
	for _, tract := range p.tracts {
		tract.SetInput(in)
	}
}

func (p *paralellGroupTract) SetOutput(out Output) {
	if len(p.tracts) == 0 {
		return
	}
	for _, tract := range p.tracts {
		tract.SetOutput(out)
	}
}

// NewFanOutGroupTract makes a new tract that consists muliple other tracts.
// Each request this tract receives is routed to all of its inner tracts.
// All requests proccessed by the inner tracts are routed to the same output.
//    -----------------
//    | / ( Tract0 ) \ |
// -> | - ( Tract1 ) - | ->
//    | \ ( Tract2 ) / |
//    |      ...      |
//    -----------------
func NewFanOutGroupTract(name string, tracts ...Tract) Tract {
	fanOutTract := &fanOutTract{
		input: InputGenerator{},
	}
	for _, tract := range tracts {
		link(fanOutTract, tract)
	}
	tract := &fanOutGroupTract{}
	tract.name = name
	tract.tracts = append(
		[]Tract{fanOutTract},
		tracts...,
	)
	return tract
}

type fanOutGroupTract struct {
	serialGroupTract
}

func (p *fanOutGroupTract) SetOutput(out Output) {
	// The first tract is always the fanOutTract, which should not be included in the true list of inner tracts the user knows/cares about.
	if len(p.tracts) <= 1 {
		return
	}
	for _, tract := range p.tracts[1:] {
		tract.SetOutput(out)
	}
}
