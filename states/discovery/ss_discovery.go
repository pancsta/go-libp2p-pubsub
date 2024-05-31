package discovery

import am "github.com/pancsta/asyncmachine-go/pkg/machine"

// S is a type alias for a list of state names.
type S = am.S

// States define relations between states.
var States = am.Struct{
	Start: {
		Add: S{PoolTimer},
	},
	PoolTimer: {},
	RefreshingDiscoveries: {
		Require: S{Start},
	},
	DiscoveriesRefreshed: {
		Require: S{Start},
	},

	// topics

	DiscoveringTopic: {
		Multi: true,
	},
	TopicDiscovered: {
		Multi: true,
	},

	BootstrappingTopic: {
		Multi: true,
	},
	TopicBootstrapped: {
		Multi: true,
	},

	AdvertisingTopic: {
		Multi: true,
	},
	StopAdvertisingTopic: {
		Multi: true,
	},
}

// StatesBootstrapFlow define relations between states for the bootstrap flow.
var StatesBootstrapFlow = am.Struct{
	Start: {
		Add: S{BootstrapChecking},
	},
	BootstrapChecking: {
		Remove: BootstrapGroup,
	},
	DiscoveringTopic: {
		Remove: BootstrapGroup,
	},
	BootstrapDelay: {
		Remove: BootstrapGroup,
	},
	TopicBootstrapped: {
		Remove: BootstrapGroup,
	},
}

// Groups of mutually exclusive states.

var (
	BootstrapGroup = S{DiscoveringTopic, BootstrapDelay, BootstrapChecking, TopicBootstrapped}
)

// Names of states

const (
	Exception = "Exception"

	Start                 = "Start"
	PoolTimer             = "PoolTimer"
	RefreshingDiscoveries = "RefreshingDiscoveries"
	DiscoveriesRefreshed  = "DiscoveriesRefreshed"

	// topics

	DiscoveringTopic = "DiscoveringTopic"
	TopicDiscovered  = "TopicDiscovered"

	BootstrappingTopic = "BootstrappingTopic"
	TopicBootstrapped  = "TopicBootstrapped"

	AdvertisingTopic     = "AdvertisingTopic"
	StopAdvertisingTopic = "StopAdvertisingTopic"

	// BootstrapFlow

	BootstrapDelay    = "BootstrapDelay"
	BootstrapChecking = "BootstrapChecking"
)

// Names collects and define an order for state names
var Names = S{Exception, Start, PoolTimer, RefreshingDiscoveries, DiscoveriesRefreshed,
	DiscoveringTopic, TopicDiscovered, BootstrappingTopic, TopicBootstrapped, AdvertisingTopic, StopAdvertisingTopic}

var NamesBootstrapFlow = S{Exception, Start, BootstrapChecking, DiscoveringTopic, BootstrapDelay, TopicBootstrapped}
