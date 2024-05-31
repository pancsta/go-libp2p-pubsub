package states

import am "github.com/pancsta/asyncmachine-go/pkg/machine"

// States define relations between states
var States = am.Struct{
	// peers
	PeersPending: {},
	PeersDead:    {},
	GetPeers:     {Multi: true},

	// peer
	PeerNewStream:   {Multi: true},
	PeerCloseStream: {Multi: true},
	PeerError:       {Multi: true},
	PublishMessage:  {Multi: true},
	BlacklistPeer:   {Multi: true},

	// topic
	GetTopics:       {Multi: true},
	AddTopic:        {Multi: true},
	RemoveTopic:     {Multi: true},
	AnnouncingTopic: {Multi: true},
	TopicAnnounced:  {Multi: true},

	// subscription
	RemoveSubscription: {Multi: true},
	AddSubscription:    {Multi: true},

	// misc
	AddRelay:        {Multi: true},
	RemoveRelay:     {Multi: true},
	IncomingRPC:     {Multi: true},
	AddValidator:    {Multi: true},
	RemoveValidator: {Multi: true},
}

// Names of states

const (
	Exception = "Exception"

	PeersPending       = "PeersPending"
	PeersDead          = "PeersDead"
	PeerNewStream      = "PeerNewStream"
	PeerCloseStream    = "PeerCloseStream"
	PeerError          = "PeerError"
	GetTopics          = "GetTopics"
	AddTopic           = "AddTopic"
	RemoveTopic        = "RemoveTopic"
	RemoveSubscription = "RemoveSubscription"
	AddSubscription    = "AddSubscription"
	AddRelay           = "AddRelay"
	RemoveRelay        = "RemoveRelay"
	GetPeers           = "GetPeers"
	IncomingRPC        = "IncomingRPC"
	AddValidator       = "AddValidator"
	RemoveValidator    = "RemoveValidator"
	PublishMessage     = "PublishMessage"
	AnnouncingTopic    = "AnnouncingTopic"
	TopicAnnounced     = "TopicAnnounced"
	BlacklistPeer      = "BlacklistPeer"
)

// Names collects and define an order for state names
var Names = am.S{Exception, PeersPending, PeersDead, PeerNewStream, PeerCloseStream, PeerError, GetTopics, AddTopic,
	RemoveTopic, RemoveSubscription, AddSubscription, AddRelay, RemoveRelay, GetPeers, IncomingRPC, AddValidator,
	RemoveValidator, PublishMessage, AnnouncingTopic, TopicAnnounced, BlacklistPeer}
