package tract

// Request is any type the User wants to pass around in a tract.
// Different Request types can be used for input and outputs to tracts and workers.
type Request interface {
	any
}
