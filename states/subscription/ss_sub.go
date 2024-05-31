package subscription

import am "github.com/pancsta/asyncmachine-go/pkg/machine"

// States define relations between states
// TODO implement?
var States = am.Struct{
	Subscribing: {},
	Subscribed:  {},
}

// Names of all the states (pkg enum).

const (
	Subscribing = "Subscribing"
	Subscribed  = "Subscribed"
)

// Names is an ordered list of all the state names.
var Names = am.S{Subscribed, Subscribing}
