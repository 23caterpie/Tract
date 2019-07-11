package tract

var (
	_ Tract = &workerTract{}
	_ Tract = &serialGroupTract{}
	_ Tract = &paralellGroupTract{}
	_ Tract = &fanOutGroupTract{}
	_ Tract = &fanOutTract{}
)

// Tract is a highly concurrent, scalable design pattern.
// Tracts receive and pass Requests from/to other tracts thus forming larger tracts.
// Each sub-tract has a job it does with the base sub-tract being a worker tract.
// A worker tract performs operations on a request before passing it along the overarching tract.
// All other tracts are group tracts that manager other sub-tracts, tracts that manager the flow
// of requests, or are advanced user implemented tracts (user will generally just implement workers).
type Tract interface {
	// Name of the Tract: used for logging and instrementation.
	Name() string
	// Init initializes the Tract. Must be called before calling Start().
	Init() error
	// Start starts the Tract. Returns a callback that waits for the Tract to finish processing.
	// Callback must be called to close resources
	Start() func()
	// setInput sets the input of the tract
	SetInput(Input)
	// setOutput sets the output of the tract
	SetOutput(Output)
}
