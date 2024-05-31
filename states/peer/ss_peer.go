package peer

import am "github.com/pancsta/asyncmachine-go/pkg/machine"

// Names of states
const (
	Publish = "Publish"
)

// Names collects and define an order for state names
var Names = am.S{Publish}

// States define relations between states
var States = am.Struct{
	//Start:       {},
	//NewStream:   {},
	//Blacklisted: {},
	//PeerError:   {},
}
