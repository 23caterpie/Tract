package tract

var (
	_ Tract = &workerTract{}
	_ Tract = &serialGroupTract{}
	_ Tract = &paralellGroupTract{}
	_ Tract = &fanOutGroupTract{}
	_ Tract = &fanOutTract{}
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
//  3. myTract is started by calling myTract.Start().
//  4. myTract is closed by calling the callback returned from Start().
//  5. myTract can be used again by looping back to step 2 (by default).
//     * Init() -> Start()() -> Init() ...
//
// A tract will close when its input specifies there are no more requests to process:
//  1. The base case first Tract is a Worker Tract. It's Worker can be viewed as the Request generator.
//     When that Worker returns a "should not send" from Work(), there are no more Request, and the Tract will shutdown.
//  2. The Tract's input has been manually set by the user. The user contols Tract shutdown using that input.
//
// Usage:
//  myTract := tract.NewXYZTract(...)
//  err := myTract.Init()
//  if err != nil {
//      // Handle error
//      return
//  }
//  waitForTract := myTract.Start()
//  waitForTract()
//
//  // Let's start again!
//  err = myTract.Init()
//  ...
type Tract interface {
	// Name of the Tract: used for logging and instrementation.
	Name() string
	// Init initializes the Tract. Must be called before calling Start().
	// Once Start has been called, Init should not be called.
	Init() error
	// Start starts the Tract. Returns a callback that waits for the Tract to finish processing.
	// Callback must be called to close resources and close output.
	Start() func()
	// SetInput sets the input of the tract.
	// Users should generally use group Tracts instead of using SetInput directly.
	// Tracts used as sub-tracts in a tract group will have thier inputs set by the group's Init()
	// in which case the groups SetInput should be used instead.
	SetInput(Input)
	// SetOutput sets the output of the tract.
	// Users should generally use group Tracts instead of using SetOutput directly.
	// Tracts used as sub-tracts in a tract group will have thier outputs set by the group's Init()
	// in which case the groups SetOutput should be used instead.
	SetOutput(Output)
}
