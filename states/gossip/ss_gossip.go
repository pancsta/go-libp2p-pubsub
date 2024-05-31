package states

import am "github.com/pancsta/asyncmachine-go/pkg/machine"

// S is a type alias for a list of state names.
type S = am.S

// States map defines relations and properties of states.
// TODO implement?
var States = am.Struct{
	HeartbeatOngoing:   {Remove: groupHeartbeatOuter},
	Heartbeat1:         {Remove: groupHeartbeatInner},
	Heartbeat2:         {Remove: groupHeartbeatInner},
	HeartbeatCompleted: {Remove: groupHeartbeatOuter},
}

// Groups of mutually exclusive states.

var (
	groupHeartbeatOuter = S{HeartbeatOngoing, HeartbeatCompleted}
	groupHeartbeatInner = S{Heartbeat1, Heartbeat2}
)

// Names of all the states (pkg enum).

const (
	HeartbeatOngoing   = "HeartbeatOngoing"
	Heartbeat1         = "Heartbeat1"
	Heartbeat2         = "Heartbeat2"
	HeartbeatCompleted = "HeartbeatCompleted"
)

// Names is an ordered list of all the state names.
var Names = S{HeartbeatOngoing, Heartbeat1, Heartbeat2, HeartbeatCompleted}
