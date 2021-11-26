package tract

var (
	_ Tract[int64, string] = &workerTract[int64, string]{}
	_ Tract[int64, string] = &SerialGroupTract[int64, bool, string]{}
	_ Tract[int64, string] = &ParalellGroupTract[int64, string]{}
	_ Tract[int64, string] = &FanOutGroupTract[int64, bool, string]{}
)

// Tract is a highly concurrent, scalable design pattern.
// Tracts receive and pass Requests from/to other Tracts. Tracts can be combined to form a single group Tract.
// Each sub-Tract in a group has a job it does with the base sub-Tract being a Worker Tract.
// A Worker Tract performs operations on a request before passing it along the overarching group Tract.
// Other than Worker Tracts, all other Tracts manage other Tracts, manage the flow of Requests, or are advanced
// user implemented Tracts (user will generally just implement workers).
//
// A Tract lifecycle is as follows:
//  1. myTract is constructed by one of the Tract contructors in this package.
//  2. myTract is initialized by calling myTract.Init().
//     * if Init() returns an error, it is not safe to proceed.
//  3. myTract is started by calling myTractStarter.Start() returned from Init().
//  4. myTract is closed by calling myTractWaiter.Wait() returned from Start().
//  5. myTract can be used multiple times using this pattern.
//     * Init() -> Start() -> Wait()
//
// A tract will close when its input specifies there are no more requests to process.
//
// Usage:
//  myTract := tract.NewXYZTract(...)
//  tractStarter, err := myTract.Init()
//  if err != nil {
//      // Handle error
//      return
//  }
//  tractWaiter := tractStarter.Start()
//  tractWaiter.Wait()
//
//  // Let's start again!
//  err = myTract.Init()
//  ...
type Tract[InputType, OutputType any] interface {
	// Name of the Tract: used for logging and instrementation.
	Name() string
	// Init initializes the Tract. Must be called before calling Start().
	// Once Start has been called, Init should not be called.
	Init(Input[InputType], Output[OutputType]) (TractStarter, error)
}

type TractStarter interface {
	// Start starts the Tract. Returns a TractWaiter that waits for the Tract to finish processing.
	// TractWaiter must be called to close resources and close output.
	Start() TractWaiter
}

type TractWaiter interface {
	// Wait waits for the Tract to finish processing
	Wait()
}

func Run[InputType, OutputType any](
	input Input[InputType],
	tract Tract[InputType, OutputType],
	output Output[OutputType],
) error {
	return NewTractRunner(input, tract, output).Run()
}

// TODO: use and comment this. Single use.
func NewTractRunner[InputType, OutputType any](
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

type TractRunner[InputType, OutputType any] struct {
	input  Input[InputType]
	tract  Tract[InputType, OutputType]
	output Output[OutputType]
}

func (t *TractRunner[InputType, OutputType]) Name() string {
	return t.tract.Name()
}

func (t *TractRunner[InputType, OutputType]) Run() error {
	starter, err := t.tract.Init(t.input, t.output)
	if err != nil {
		return err
	}
	starter.Start().Wait()
	return nil
}

// internal function wrappers

var _ TractStarter = tractStarterFunc(nil)

type tractStarterFunc func() TractWaiter

func (f tractStarterFunc) Start() TractWaiter {
	return f()
}

var _ TractWaiter = tractWaiterFunc(nil)

type tractWaiterFunc func()

func (f tractWaiterFunc) Wait() {
	f()
}
