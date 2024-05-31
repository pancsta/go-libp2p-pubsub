package topic

import am "github.com/pancsta/asyncmachine-go/pkg/machine"

// S is a type alias for a list of state names.
type S = am.S

// States map defines relations and properties of states.
// TODO implement?
var States = am.Struct{
	Publish: {
		Multi: true,
	},
	Closed: {
		Remove: am.S{Subscribe, Advertise},
	},
	Subscribe: {
		Multi: true,
		Add:   am.S{Discovering},
	},
	Advertise: {
		Multi: true,
	},
	Discovering: {
		Remove: discovering,
	},
	Discovered: {
		Remove: discovering,
	},
	EnoughPeers: {},
	FindPeers:   {},
}

// Groups of mutually exclusive states.

var (
	discovering = am.S{Discovering, Discovered}
)

// Names of all the states (pkg enum).

const (
	Publish = "Publish"

	Closed      = "Closed"
	Subscribe   = "Subscribe"
	Advertise   = "Advertise"
	Discovering = "Discovering"
	Discovered  = "Discovered"
	EnoughPeers = "EnoughPeers"
	FindPeers   = "FindPeers"
)

// Names is an ordered list of all the state names.
var Names = am.S{Publish}
