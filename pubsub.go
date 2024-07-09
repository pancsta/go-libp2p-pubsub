package pubsub

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/pancsta/go-libp2p-pubsub/pb"
	ss "github.com/pancsta/go-libp2p-pubsub/states/pubsub"
	"github.com/pancsta/go-libp2p-pubsub/timecache"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	logging "github.com/ipfs/go-log/v2"
	am "github.com/pancsta/asyncmachine-go/pkg/machine"
	"github.com/pancsta/asyncmachine-go/pkg/telemetry"
)

// DefaultMaximumMessageSize is 1mb.
const DefaultMaxMessageSize = 1 << 20

var (
	// TimeCacheDuration specifies how long a message ID will be remembered as seen.
	// Use WithSeenMessagesTTL to configure this per pubsub instance, instead of overriding the global default.
	TimeCacheDuration = 120 * time.Second

	// TimeCacheStrategy specifies which type of lookup/cleanup strategy is used by the seen messages cache.
	// Use WithSeenMessagesStrategy to configure this per pubsub instance, instead of overriding the global default.
	TimeCacheStrategy = timecache.Strategy_FirstSeen

	// ErrSubscriptionCancelled may be returned when a subscription Next() is called after the
	// subscription has been cancelled.
	ErrSubscriptionCancelled = errors.New("subscription cancelled")
)

var log = logging.Logger("pubsub")

type ProtocolMatchFn = func(protocol.ID) func(protocol.ID) bool

// PubSub is the implementation of the pubsub system.
type PubSub struct {
	// atomic counter for seqnos
	// NOTE: Must be declared at the top of the struct as we perform atomic
	// operations on this field.
	//
	// See: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	counter uint64

	host host.Host

	rt PubSubRouter

	val *validation

	disc *discover

	tracer *pubsubTracer

	peerFilter PeerFilter

	// maxMessageSize is the maximum message size; it applies globally to all
	// topics.
	maxMessageSize int

	// size of the outbound message channel that we maintain for each peer
	peerOutboundQueueSize int

	// incoming messages from other peers
	incoming chan *RPC

	// send subscription here to cancel it
	cancelCh chan *Subscription

	// a notification channel for new peer connections accumulated
	newPeers       chan struct{}
	newPeersPrioLk sync.RWMutex
	newPeersMx     sync.Mutex
	newPeersPend   map[peer.ID]struct{}

	// a notification channel for when our peers die
	peerDead       chan struct{}
	peerDeadPrioLk sync.RWMutex
	peerDeadMx     sync.Mutex
	peerDeadPend   map[peer.ID]struct{}
	// backoff for retrying new connections to dead peers
	deadPeerBackoff *backoff

	// The set of topics we are subscribed to
	mySubs map[string]map[*Subscription]struct{}

	// The set of topics we are relaying for
	myRelays map[string]int

	// The set of topics we are interested in
	myTopics map[string]*Topic

	// topics tracks which topics each of our peers are subscribed to
	topics map[string]map[peer.ID]struct{}

	// peer blacklist
	blacklist Blacklist

	peers map[peer.ID]chan *RPC

	inboundStreamsMx sync.Mutex
	inboundStreams   map[peer.ID]network.Stream

	seenMessages    timecache.TimeCache
	seenMsgTTL      time.Duration
	seenMsgStrategy timecache.Strategy

	// generator used to compute the ID for a message
	idGen *msgIDGenerator

	// key for signing messages; nil when signing is disabled
	signKey crypto.PrivKey
	// source ID for signed messages; corresponds to signKey, empty when signing is disabled.
	// If empty, the author and seq-nr are completely omitted from the messages.
	signID peer.ID
	// strict mode rejects all unsigned messages prior to validation
	signPolicy MessageSignaturePolicy

	// filter for tracking subscriptions in topics of interest; if nil, then we track all subscriptions
	subFilter SubscriptionFilter

	// protoMatchFunc is a matching function for protocol selection.
	protoMatchFunc ProtocolMatchFn

	ctx context.Context

	// appSpecificRpcInspector is an auxiliary that may be set by the application to inspect incoming RPCs prior to
	// processing them. The inspector is invoked on an accepted RPC right prior to handling it.
	// The return value of the inspector function is an error indicating whether the RPC should be processed or not.
	// If the error is nil, the RPC is processed as usual. If the error is non-nil, the RPC is dropped.
	appSpecificRpcInspector func(peer.ID, *RPC) error

	Mach *am.Machine
	*am.ExceptionHandler
}

// PubSubRouter is the message router component of PubSub.
// TODO asyncmachine
type PubSubRouter interface {
	// Protocols returns the list of protocols supported by the router.
	Protocols() []protocol.ID
	// Attach is invoked by the PubSub constructor to attach the router to a
	// freshly initialized PubSub instance.
	Attach(*PubSub)
	// AddPeer notifies the router that a new peer has been connected.
	AddPeer(peer.ID, protocol.ID)
	// RemovePeer notifies the router that a peer has been disconnected.
	RemovePeer(peer.ID)
	// EnoughPeers returns whether the router needs more peers before it's ready to publish new records.
	// Suggested (if greater than 0) is a suggested number of peers that the router should need.
	EnoughPeers(topic string, suggested int) bool
	// AcceptFrom is invoked on any incoming message before pushing it to the validation pipeline
	// or processing control information.
	// Allows routers with internal scoring to vet peers before committing any processing resources
	// to the message and implement an effective graylist and react to validation queue overload.
	AcceptFrom(peer.ID) AcceptStatus
	// HandleRPC is invoked to process control messages in the RPC envelope.
	// It is invoked after subscriptions and payload messages have been processed.
	HandleRPC(*RPC)
	// Publish is invoked to forward a new message that has been validated.
	Publish(*Message)
	// Join notifies the router that we want to receive and forward messages in a topic.
	// It is invoked after the subscription announcement.
	Join(topic string)
	// Leave notifies the router that we are no longer interested in a topic.
	// It is invoked after the unsubscription announcement.
	Leave(topic string)
}

type AcceptStatus int

const (
	// AcceptNone signals to drop the incoming RPC
	AcceptNone AcceptStatus = iota
	// AcceptControl signals to accept the incoming RPC only for control message processing by
	// the router. Included payload messages will _not_ be pushed to the validation queue.
	AcceptControl
	// AcceptAll signals to accept the incoming RPC for full processing
	AcceptAll
)

type Message struct {
	*pb.Message
	ID            string
	ReceivedFrom  peer.ID
	ValidatorData interface{}
	Local         bool
}

func (m *Message) GetFrom() peer.ID {
	return peer.ID(m.Message.GetFrom())
}

type RPC struct {
	pb.RPC

	// unexported on purpose, not sending this over the wire
	from peer.ID
}

type Option func(*PubSub) error

// NewPubSub returns a new PubSub management object.
func NewPubSub(ctx context.Context, h host.Host, rt PubSubRouter, opts ...Option) (*PubSub, error) {
	ps := &PubSub{
		host:                  h,
		ctx:                   ctx,
		rt:                    rt,
		val:                   newValidation(),
		peerFilter:            DefaultPeerFilter,
		disc:                  &discover{},
		maxMessageSize:        DefaultMaxMessageSize,
		peerOutboundQueueSize: 32,
		signID:                h.ID(),
		signKey:               nil,
		signPolicy:            StrictSign,
		incoming:              make(chan *RPC, 32),
		newPeersPend:          make(map[peer.ID]struct{}),
		peerDeadPend:          make(map[peer.ID]struct{}),
		deadPeerBackoff:       newBackoff(ctx, 1000, BackoffCleanupInterval, MaxBackoffAttempts),
		cancelCh:              make(chan *Subscription),
		myTopics:              make(map[string]*Topic),
		mySubs:                make(map[string]map[*Subscription]struct{}),
		myRelays:              make(map[string]int),
		topics:                make(map[string]map[peer.ID]struct{}),
		peers:                 make(map[peer.ID]chan *RPC),
		inboundStreams:        make(map[peer.ID]network.Stream),
		blacklist:             NewMapBlacklist(),
		seenMsgTTL:            TimeCacheDuration,
		seenMsgStrategy:       TimeCacheStrategy,
		idGen:                 newMsgIdGenerator(),
		counter:               uint64(time.Now().UnixNano()),
	}

	err := ps.initMachine()
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		err := opt(ps)
		if err != nil {
			return nil, err
		}
	}

	if ps.signPolicy.mustSign() {
		if ps.signID == "" {
			return nil, fmt.Errorf("strict signature usage enabled but message author was disabled")
		}
		ps.signKey = ps.host.Peerstore().PrivKey(ps.signID)
		if ps.signKey == nil {
			return nil, fmt.Errorf("can't sign for peer %s: no private key", ps.signID)
		}
	}

	ps.seenMessages = timecache.NewTimeCacheWithStrategy(ps.seenMsgStrategy, ps.seenMsgTTL)

	if err := ps.disc.Start(ps); err != nil {
		return nil, err
	}

	rt.Attach(ps)

	for _, id := range rt.Protocols() {
		if ps.protoMatchFunc != nil {
			h.SetStreamHandlerMatch(id, ps.protoMatchFunc(id), ps.handleNewStream)
		} else {
			h.SetStreamHandler(id, ps.handleNewStream)
		}
	}
	h.Network().Notify((*PubSubNotif)(ps))

	ps.val.Start(ps)

	go ps.processLoop(ctx)

	(*PubSubNotif)(ps).Initialize()

	return ps, nil
}

// MsgIdFunction returns a unique ID for the passed Message, and PubSub can be customized to use any
// implementation of this function by configuring it with the Option from WithMessageIdFn.
type MsgIdFunction func(pmsg *pb.Message) string

// WithMessageIdFn is an option to customize the way a message ID is computed for a pubsub message.
// The default ID function is DefaultMsgIdFn (concatenate source and seq nr.),
// but it can be customized to e.g. the hash of the message.
func WithMessageIdFn(fn MsgIdFunction) Option {
	return func(p *PubSub) error {
		p.idGen.Default = fn
		return nil
	}
}

// PeerFilter is used to filter pubsub peers. It should return true for peers that are accepted for
// a given topic. PubSub can be customized to use any implementation of this function by configuring
// it with the Option from WithPeerFilter.
type PeerFilter func(pid peer.ID, topic string) bool

// WithPeerFilter is an option to set a filter for pubsub peers.
// The default peer filter is DefaultPeerFilter (which always returns true), but it can be customized
// to any custom implementation.
func WithPeerFilter(filter PeerFilter) Option {
	return func(p *PubSub) error {
		p.peerFilter = filter
		return nil
	}
}

// WithPeerOutboundQueueSize is an option to set the buffer size for outbound messages to a peer
// We start dropping messages to a peer if the outbound queue if full
func WithPeerOutboundQueueSize(size int) Option {
	return func(p *PubSub) error {
		if size <= 0 {
			return errors.New("outbound queue size must always be positive")
		}
		p.peerOutboundQueueSize = size
		return nil
	}
}

// WithMessageSignaturePolicy sets the mode of operation for producing and verifying message signatures.
func WithMessageSignaturePolicy(policy MessageSignaturePolicy) Option {
	return func(p *PubSub) error {
		p.signPolicy = policy
		return nil
	}
}

// WithMessageSigning enables or disables message signing (enabled by default).
// Deprecated: signature verification without message signing,
// or message signing without verification, are not recommended.
func WithMessageSigning(enabled bool) Option {
	return func(p *PubSub) error {
		if enabled {
			p.signPolicy |= msgSigning
		} else {
			p.signPolicy &^= msgSigning
		}
		return nil
	}
}

// WithMessageAuthor sets the author for outbound messages to the given peer ID
// (defaults to the host's ID). If message signing is enabled, the private key
// must be available in the host's peerstore.
func WithMessageAuthor(author peer.ID) Option {
	return func(p *PubSub) error {
		author := author
		if author == "" {
			author = p.host.ID()
		}
		p.signID = author
		return nil
	}
}

// WithNoAuthor omits the author and seq-number data of messages, and disables the use of signatures.
// Not recommended to use with the default message ID function, see WithMessageIdFn.
func WithNoAuthor() Option {
	return func(p *PubSub) error {
		p.signID = ""
		p.signPolicy &^= msgSigning
		return nil
	}
}

// WithStrictSignatureVerification is an option to enable or disable strict message signing.
// When enabled (which is the default), unsigned messages will be discarded.
// Deprecated: signature verification without message signing,
// or message signing without verification, are not recommended.
func WithStrictSignatureVerification(required bool) Option {
	return func(p *PubSub) error {
		if required {
			p.signPolicy |= msgVerification
		} else {
			p.signPolicy &^= msgVerification
		}
		return nil
	}
}

// WithBlacklist provides an implementation of the blacklist; the default is a
// MapBlacklist
func WithBlacklist(b Blacklist) Option {
	return func(p *PubSub) error {
		p.blacklist = b
		return nil
	}
}

// WithDiscovery provides a discovery mechanism used to bootstrap and provide peers into PubSub
func WithDiscovery(d discovery.Discovery, opts ...DiscoverOpt) Option {
	return func(p *PubSub) error {
		discoverOpts := defaultDiscoverOptions()
		for _, opt := range opts {
			err := opt(discoverOpts)
			if err != nil {
				return err
			}
		}

		p.disc.discovery = &pubSubDiscovery{Discovery: d, opts: discoverOpts.opts}
		p.disc.options = discoverOpts
		return nil
	}
}

// WithEventTracer provides a tracer for the pubsub system
func WithEventTracer(tracer EventTracer) Option {
	return func(p *PubSub) error {
		if p.tracer != nil {
			p.tracer.tracer = tracer
		} else {
			p.tracer = &pubsubTracer{tracer: tracer, pid: p.host.ID(), idGen: p.idGen}
		}
		return nil
	}
}

// WithRawTracer adds a raw tracer to the pubsub system.
// Multiple tracers can be added using multiple invocations of the option.
func WithRawTracer(tracer RawTracer) Option {
	return func(p *PubSub) error {
		if p.tracer != nil {
			p.tracer.raw = append(p.tracer.raw, tracer)
		} else {
			p.tracer = &pubsubTracer{raw: []RawTracer{tracer}, pid: p.host.ID(), idGen: p.idGen}
		}
		return nil
	}
}

// WithMaxMessageSize sets the global maximum message size for pubsub wire
// messages. The default value is 1MiB (DefaultMaxMessageSize).
//
// Observe the following warnings when setting this option.
//
// WARNING #1: Make sure to change the default protocol prefixes for floodsub
// (FloodSubID) and gossipsub (GossipSubID). This avoids accidentally joining
// the public default network, which uses the default max message size, and
// therefore will cause messages to be dropped.
//
// WARNING #2: Reducing the default max message limit is fine, if you are
// certain that your application messages will not exceed the new limit.
// However, be wary of increasing the limit, as pubsub networks are naturally
// write-amplifying, i.e. for every message we receive, we send D copies of the
// message to our peers. If those messages are large, the bandwidth requirements
// will grow linearly. Note that propagation is sent on the uplink, which
// traditionally is more constrained than the downlink. Instead, consider
// out-of-band retrieval for large messages, by sending a CID (Content-ID) or
// another type of locator, such that messages can be fetched on-demand, rather
// than being pushed proactively. Under this design, you'd use the pubsub layer
// as a signalling system, rather than a data delivery system.
func WithMaxMessageSize(maxMessageSize int) Option {
	return func(ps *PubSub) error {
		ps.maxMessageSize = maxMessageSize
		return nil
	}
}

// WithProtocolMatchFn sets a custom matching function for protocol selection to
// be used by the protocol handler on the Host's Mux. Should be combined with
// WithGossipSubProtocols feature function for checking if certain protocol features
// are supported
func WithProtocolMatchFn(m ProtocolMatchFn) Option {
	return func(ps *PubSub) error {
		ps.protoMatchFunc = m
		return nil
	}
}

// WithSeenMessagesTTL configures when a previously seen message ID can be forgotten about
func WithSeenMessagesTTL(ttl time.Duration) Option {
	return func(ps *PubSub) error {
		ps.seenMsgTTL = ttl
		return nil
	}
}

// WithSeenMessagesStrategy configures which type of lookup/cleanup strategy is used by the seen messages cache
func WithSeenMessagesStrategy(strategy timecache.Strategy) Option {
	return func(ps *PubSub) error {
		ps.seenMsgStrategy = strategy
		return nil
	}
}

// WithAppSpecificRpcInspector sets a hook that inspect incomings RPCs prior to
// processing them.  The inspector is invoked on an accepted RPC just before it
// is handled.  If inspector's error is nil, the RPC is handled. Otherwise, it
// is dropped.
func WithAppSpecificRpcInspector(inspector func(peer.ID, *RPC) error) Option {
	return func(ps *PubSub) error {
		ps.appSpecificRpcInspector = inspector
		return nil
	}
}

// processLoop handles all inputs arriving on the channels
func (p *PubSub) processLoop(ctx context.Context) {
	defer func() {

		// wait for a full disposal
		<-p.Mach.WhenDisposed()
		if psmon.Log != nil {
			psmon.Log.Printf("pubsub disposed %s\n", p.Mach.ID)
		}

		// Clean up go routines.
		for _, ch := range p.peers {
			close(ch)
		}
		p.peers = nil
		p.topics = nil
		p.seenMessages.Done()
	}()

	for {
		select {
		case rpc := <-p.incoming:
			p.handleIncomingRPC(rpc)

		case <-ctx.Done():
			log.Info("pubsub processloop shutting down")
			return
		}
	}
}

// ///// ///// /////
// ///// HANDLERS
// ///// ///// /////

func (p *PubSub) PeersPendingState(e *am.Event) {
	p.Mach.Remove1(ss.PeersPending, nil)

	p.newPeersPrioLk.Lock()

	if len(p.newPeersPend) == 0 {
		p.newPeersPrioLk.Unlock()
		return
	}

	newPeers := p.newPeersPend
	p.newPeersPend = make(map[peer.ID]struct{})
	p.newPeersPrioLk.Unlock()

	for pid := range newPeers {
		if p.host.Network().Connectedness(pid) != network.Connected {
			continue
		}

		if _, ok := p.peers[pid]; ok {
			log.Debug("already have connection to peer: ", pid)
			continue
		}

		if p.blacklist.Contains(pid) {
			log.Warn("ignoring connection from blacklisted peer: ", pid)
			continue
		}

		messages := make(chan *RPC, p.peerOutboundQueueSize)
		messages <- p.getHelloPacket()
		go p.handleNewPeer(p.ctx, pid, messages)
		p.peers[pid] = messages
	}

}

func (p *PubSub) PeerNewStreamState(e *am.Event) {
	p.Mach.Remove1(ss.PeerNewStream, nil)

	s := e.Args["network.Stream"].(network.Stream)
	pid := s.Conn().RemotePeer()

	ch, ok := p.peers[pid]
	if !ok {
		log.Warn("new stream for unknown peer: ", pid)
		s.Reset()
	}

	if p.blacklist.Contains(pid) {
		log.Warn("closing stream for blacklisted peer: ", pid)
		close(ch)
		delete(p.peers, pid)
		s.Reset()
	}

	p.rt.AddPeer(pid, s.Protocol())
}

func (p *PubSub) PeerErrorState(e *am.Event) {
	p.Mach.Remove1(ss.PeerError, nil)

	pid := e.Args["pid"].(peer.ID)
	delete(p.peers, pid)
}

func (p *PubSub) PeersDeadEnter(e *am.Event) bool {
	return len(p.peerDeadPend) > 0
}

func (p *PubSub) PeersDeadState(e *am.Event) {
	p.Mach.Remove1(ss.PeersDead, nil)

	deadPeers := p.peerDeadPend
	p.peerDeadPend = make(map[peer.ID]struct{})

	for pid := range deadPeers {
		ch, ok := p.peers[pid]
		if !ok {
			continue
		}

		close(ch)
		delete(p.peers, pid)

		for t, tmap := range p.topics {
			if _, ok := tmap[pid]; ok {
				delete(tmap, pid)
				p.notifyLeave(t, pid)
			}
		}

		p.rt.RemovePeer(pid)

		if p.host.Network().Connectedness(pid) == network.Connected {
			backoffDelay, err := p.deadPeerBackoff.updateAndGet(pid)
			if err != nil {
				log.Debug(err)
				continue
			}

			// still connected, must be a duplicate connection being closed.
			// we respawn the writer as we need to ensure there is a stream active
			log.Debugf("peer declared dead but still connected; respawning writer: %s", pid)
			messages := make(chan *RPC, p.peerOutboundQueueSize)
			messages <- p.getHelloPacket()
			p.peers[pid] = messages
			go p.handleNewPeerWithBackoff(p.ctx, pid, backoffDelay, messages)
		}
	}
}

func (p *PubSub) GetTopicsEnter(e *am.Event) bool {
	_, ok := e.Args["topicReq"].(*topicReq)
	return ok
}

func (p *PubSub) GetTopicsState(e *am.Event) {
	p.Mach.Remove1(ss.GetTopics, nil)

	req := e.Args["topicReq"].(*topicReq)
	var out []string
	for t := range p.mySubs {
		out = append(out, t)
	}
	// buffered
	req.resp <- out
}

func (p *PubSub) AddTopicEnter(e *am.Event) bool {
	_, ok := e.Args["addTopicReq"].(*addTopicReq)
	return ok
}

// AddTopicState adds a tracker for a particular topic.
// Only called from processLoop.
func (p *PubSub) AddTopicState(e *am.Event) {
	p.Mach.Remove1(ss.AddTopic, nil)

	req := e.Args["addTopicReq"].(*addTopicReq)
	topic := req.topic
	topicID := topic.topic

	t, ok := p.myTopics[topicID]
	if ok {
		// buffered
		req.resp <- t
		return
	}

	p.myTopics[topicID] = topic
	// buffered
	req.resp <- topic
}

func (p *PubSub) RemoveTopicEnter(e *am.Event) bool {
	_, ok := e.Args["rmTopicReq"].(*rmTopicReq)
	return ok
}

// RemoveTopicState removes Topic tracker from bookkeeping.
// Only called from processLoop.
func (p *PubSub) RemoveTopicState(e *am.Event) {
	p.Mach.Remove1(ss.RemoveTopic, nil)

	req := e.Args["rmTopicReq"].(*rmTopicReq)
	topic := p.myTopics[req.topic.topic]

	if topic == nil {
		// buffered
		req.resp <- nil
		return
	}

	if len(topic.evtHandlers) == 0 &&
		len(p.mySubs[req.topic.topic]) == 0 &&
		p.myRelays[req.topic.topic] == 0 {
		delete(p.myTopics, topic.topic)
		// buffered channels only
		req.resp <- nil
		return
	}

	// buffered
	req.resp <- fmt.Errorf("cannot close topic: outstanding event handlers or subscriptions")
}

func (p *PubSub) RemoveSubscriptionEnter(e *am.Event) bool {
	sub, ok1 := e.Args["Subscription"].(*Subscription)
	_, ok2 := p.mySubs[sub.topic]
	return ok1 && ok2
}

// RemoveSubscriptionState removes Subscription sub from bookeeping.
// If this was the last subscription and no more relays exist for a given topic,
// it will also announce that this node is not subscribing to this topic anymore.
// Only called from processLoop.
func (p *PubSub) RemoveSubscriptionState(e *am.Event) {
	p.Mach.Remove1(ss.RemoveSubscription, nil)

	sub := e.Args["Subscription"].(*Subscription)
	subs := p.mySubs[sub.topic]

	sub.err = ErrSubscriptionCancelled
	sub.close()
	delete(subs, sub)

	if len(subs) == 0 {
		delete(p.mySubs, sub.topic)

		// stop announcing only if there are no more subs and relays
		if p.myRelays[sub.topic] == 0 {
			p.disc.StopAdvertise(sub.topic)
			p.Mach.Add1(ss.AnnouncingTopic, am.A{
				"topic":    sub.topic,
				"sub.bool": false,
			})
			p.rt.Leave(sub.topic)
		}
	}
}

func (p *PubSub) AddSubscriptionEnter(e *am.Event) bool {
	_, ok1 := e.Args["addSubReq"].(*addSubReq)
	return ok1
}

// AddSubscriptionState adds a Subscription for a particular topic. If it is
// the first subscription and no relays exist so far for the topic, it will
// announce that this node subscribes to the topic.
// Only called from processLoop.
func (p *PubSub) AddSubscriptionState(e *am.Event) {
	p.Mach.Remove1(ss.AddSubscription, nil)

	req := e.Args["addSubReq"].(*addSubReq)
	sub := req.sub
	subs := p.mySubs[sub.topic]

	// announce we want this topic if neither subs nor relays exist so far
	if len(subs) == 0 && p.myRelays[sub.topic] == 0 {
		p.disc.Advertise(sub.topic)
		p.Mach.Add1(ss.AnnouncingTopic, am.A{
			"topic":    sub.topic,
			"sub.bool": true,
		})
		p.rt.Join(sub.topic)
	}

	// make new if not there
	if subs == nil {
		p.mySubs[sub.topic] = make(map[*Subscription]struct{})
	}

	p.mySubs[sub.topic][sub] = struct{}{}

	// buffered
	req.resp <- sub
}

func (p *PubSub) AddRelayEnter(e *am.Event) bool {
	_, ok := e.Args["addRelayReq"].(*addRelayReq)
	return ok
}

// AddRelayState adds a relay for a particular topic. If it is
// the first relay and no subscriptions exist so far for the topic , it will
// announce that this node relays for the topic.
// Only called from processLoop.
func (p *PubSub) AddRelayState(e *am.Event) {
	req := e.Args["addRelayReq"].(*addRelayReq)
	topic := req.topic

	p.myRelays[topic]++

	// announce we want this topic if neither relays nor subs exist so far
	if p.myRelays[topic] == 1 && len(p.mySubs[topic]) == 0 {
		p.disc.Advertise(topic)
		p.Mach.Add1(ss.AnnouncingTopic, am.A{
			"topic":    topic,
			"sub.bool": true,
		})
		p.rt.Join(topic)
	}

	// flag used to prevent calling cancel function multiple times
	isCancelled := false

	relayCancelFunc := func() {
		if isCancelled {
			return
		}

		isCancelled = true
		p.Mach.Add1(ss.RemoveRelay, am.A{"topic": topic})
	}

	// buffered
	req.resp <- relayCancelFunc
	p.Mach.Remove1(ss.AddRelay, nil)
}

func (p *PubSub) RemoveRelayEnter(e *am.Event) bool {
	topic, ok := e.Args["topic"].(string)
	return ok && p.myRelays[topic] > 0
}

// RemoveRelayState removes one relay reference from bookkeeping.
// If this was the last relay reference and no more subscriptions exist
// for a given topic, it will also announce that this node is not relaying
// for this topic anymore.
// Only called from processLoop.
func (p *PubSub) RemoveRelayState(e *am.Event) {
	p.Mach.Remove1(ss.RemoveRelay, nil)

	topic := e.Args["topic"].(string)
	p.myRelays[topic]--

	if p.myRelays[topic] == 0 {
		delete(p.myRelays, topic)

		// stop announcing only if there are no more relays and subs
		if len(p.mySubs[topic]) == 0 {
			p.disc.StopAdvertise(topic)
			p.Mach.Add1(ss.AnnouncingTopic, am.A{
				"topic":    topic,
				"sub.bool": false,
			})
			p.rt.Leave(topic)
		}
	}
}

func (p *PubSub) GetPeersEnter(e *am.Event) bool {
	_, ok := e.Args["listPeerReq"].(*listPeerReq)
	return ok
}

func (p *PubSub) GetPeersState(e *am.Event) {
	p.Mach.Remove1(ss.GetPeers, nil)

	req := e.Args["listPeerReq"].(*listPeerReq)
	topic := req.topic

	tmap, ok := p.topics[topic]
	if topic != "" && !ok {
		// buffered
		req.resp <- nil
		return
	}

	var out []peer.ID
	for p := range p.peers {
		if topic != "" {
			_, ok := tmap[p]
			if !ok {
				continue
			}
		}
		out = append(out, p)
	}

	req.resp <- out
}

func (p *PubSub) PublishMessageEnter(e *am.Event) bool {
	_, ok := e.Args["Message"].(*Message)
	return ok
}

func (p *PubSub) PublishMessageState(e *am.Event) {
	p.Mach.Remove1(ss.PublishMessage, nil)

	msg := e.Args["Message"].(*Message)
	p.tracer.DeliverMessage(msg)
	p.notifySubs(msg)
	if !msg.Local {
		p.rt.Publish(msg)
	}
}

func (p *PubSub) AddValidatorEnter(e *am.Event) bool {
	_, ok := e.Args["addValReq"].(*addValReq)
	return ok
}

func (p *PubSub) AddValidatorState(e *am.Event) {
	p.Mach.Remove1(ss.AddValidator, nil)

	req := e.Args["addValReq"].(*addValReq)
	p.val.AddValidator(req)
}

func (p *PubSub) RemoveValidatorEnter(e *am.Event) bool {
	_, ok := e.Args["rmValReq"].(*rmValReq)
	return ok
}

func (p *PubSub) RemoveValidatorState(e *am.Event) {
	p.Mach.Remove1(ss.RemoveValidator, nil)

	req := e.Args["rmValReq"].(*rmValReq)
	p.val.RemoveValidator(req)
}

func (p *PubSub) BlacklistPeerEnter(e *am.Event) bool {
	_, ok := e.Args["pid"].(peer.ID)
	return ok
}

func (p *PubSub) BlacklistPeerState(e *am.Event) {
	p.Mach.Remove1(ss.BlacklistPeer, nil)

	pid := e.Args["pid"].(peer.ID)
	log.Infof("Blacklisting peer %s", pid)
	p.blacklist.Add(pid)

	ch, ok := p.peers[pid]
	if ok {
		close(ch)
		delete(p.peers, pid)
		for t, tmap := range p.topics {
			if _, ok := tmap[pid]; ok {
				delete(tmap, pid)
				p.notifyLeave(t, pid)
			}
		}
		p.rt.RemovePeer(pid)
	}
}

func (p *PubSub) AnnouncingTopicEnter(e *am.Event) bool {
	_, ok1 := e.Args["topic"].(string)
	_, ok2 := e.Args["sub.bool"].(bool)
	return ok1 && ok2
}

// AnnouncingTopicState announces whether or not this node is interested in a given topic
// Only called from processLoop, ends with TopicAnnouncedState.
// TODO bind to topic's context
func (p *PubSub) AnnouncingTopicState(e *am.Event) {
	p.Mach.Remove1(ss.AnnouncingTopic, nil)

	topic := e.Args["topic"].(string)
	sub := e.Args["sub.bool"].(bool)
	subopt := &pb.RPC_SubOpts{
		Topicid:   &topic,
		Subscribe: &sub,
	}

	go func() {
		out := rpcWithSubs(subopt)
		for pid, peerCh := range p.peers {
			select {
			case peerCh <- out:
				p.tracer.SendRPC(out, pid)
			default:
				log.Infof("Can't send announce message to peer %s: queue full; scheduling retry", pid)
				p.tracer.DropRPC(out, pid)
				go p.announceRetry(pid, topic, sub)
			}
		}

		p.Mach.Add1(ss.TopicAnnounced, am.A{"topic": topic})
	}()
}

func (p *PubSub) TopicAnnouncedState(e *am.Event) {
	p.Mach.Remove1(ss.TopicAnnounced, nil)
}

// ///// ///// /////
// ///// METHODS
// ///// ///// /////

func (p *PubSub) announceRetry(pid peer.ID, topic string, sub bool) {
	time.Sleep(time.Duration(1+rand.Intn(1000)) * time.Millisecond)

	retry := func() {
		_, okSubs := p.mySubs[topic]
		_, okRelays := p.myRelays[topic]

		ok := okSubs || okRelays

		if (ok && sub) || (!ok && !sub) {
			p.doAnnounceRetry(pid, topic, sub)
		}
	}

	p.Mach.Eval("announceRetry", retry, nil)
}

func (p *PubSub) doAnnounceRetry(pid peer.ID, topic string, sub bool) {
	peer, ok := p.peers[pid]
	if !ok {
		return
	}

	subopt := &pb.RPC_SubOpts{
		Topicid:   &topic,
		Subscribe: &sub,
	}

	out := rpcWithSubs(subopt)
	select {
	case peer <- out:
		p.tracer.SendRPC(out, pid)
		p.Mach.Add1(ss.TopicAnnounced, am.A{"topic": topic, "sub.bool": sub})
	default:
		log.Infof("Can't send announce message to peer %s: queue full; scheduling retry", pid)
		p.tracer.DropRPC(out, pid)
		go p.announceRetry(pid, topic, sub)
	}
}

// notifySubs sends a given message to all corresponding subscribers.
// Only called from processLoop.
func (p *PubSub) notifySubs(msg *Message) {
	topic := msg.GetTopic()
	subs := p.mySubs[topic]
	for f := range subs {
		select {
		case f.ch <- msg:
		default:
			p.tracer.UndeliverableMessage(msg)
			log.Infof("Can't deliver message to subscription for topic %s; subscriber too slow", topic)
		}
	}
}

// seenMessage returns whether we already saw this message before
func (p *PubSub) seenMessage(id string) bool {
	return p.seenMessages.Has(id)
}

// markSeen marks a message as seen such that seenMessage returns `true' for the given id
// returns true if the message was freshly marked
func (p *PubSub) markSeen(id string) bool {
	return p.seenMessages.Add(id)
}

// subscribedToMessage returns whether we are subscribed to one of the topics
// of a given message
func (p *PubSub) subscribedToMsg(msg *pb.Message) bool {
	if len(p.mySubs) == 0 {
		return false
	}

	topic := msg.GetTopic()
	_, ok := p.mySubs[topic]

	return ok
}

// canRelayMsg returns whether we are able to relay for one of the topics
// of a given message
func (p *PubSub) canRelayMsg(msg *pb.Message) bool {
	if len(p.myRelays) == 0 {
		return false
	}

	topic := msg.GetTopic()
	relays := p.myRelays[topic]

	return relays > 0
}

func (p *PubSub) notifyLeave(topic string, pid peer.ID) {
	if t, ok := p.myTopics[topic]; ok {
		t.sendNotification(PeerEvent{PeerLeave, pid})
	}
}

func (p *PubSub) handleIncomingRPC(rpc *RPC) {
	// run on machine's queue (blocks until done)
	p.Mach.Eval("handleIncomingRPC", func() {

		// pass the rpc through app specific validation (if any available).
		if p.appSpecificRpcInspector != nil {
			// check if the RPC is allowed by the external inspector
			if err := p.appSpecificRpcInspector(rpc.from, rpc); err != nil {
				log.Debugf("application-specific inspection failed, rejecting incoming rpc: %s", err)
				return // reject the RPC
			}
		}

		p.tracer.RecvRPC(rpc)

		subs := rpc.GetSubscriptions()
		if len(subs) != 0 && p.subFilter != nil {
			var err error
			subs, err = p.subFilter.FilterIncomingSubscriptions(rpc.from, subs)
			if err != nil {
				log.Debugf("subscription filter error: %s; ignoring RPC", err)
				return
			}
		}

		for _, subopt := range subs {
			t := subopt.GetTopicid()

			if subopt.GetSubscribe() {
				var toNotify []*Topic

				tmap, ok := p.topics[t]
				if !ok {
					tmap = make(map[peer.ID]struct{})
					p.topics[t] = tmap
				}
				if _, ok := tmap[rpc.from]; !ok {
					tmap[rpc.from] = struct{}{}
					if topic, ok := p.myTopics[t]; ok {
						toNotify = append(toNotify, topic)
					}
				}

				for _, topic := range toNotify {
					topic.sendNotification(PeerEvent{PeerJoin, rpc.from})
				}

			} else {
				tmap, ok := p.topics[t]
				if !ok {
					continue
				}

				if _, ok := tmap[rpc.from]; ok {
					delete(tmap, rpc.from)
					p.notifyLeave(t, rpc.from)
				}
			}
		}

		// ask the router to vet the peer before commiting any processing resources
		switch p.rt.AcceptFrom(rpc.from) {
		case AcceptNone:
			log.Debugf("received RPC from router graylisted peer %s; dropping RPC", rpc.from)
			return

		case AcceptControl:
			if len(rpc.GetPublish()) > 0 {
				log.Debugf("peer %s was throttled by router; ignoring %d payload messages", rpc.from, len(rpc.GetPublish()))
			}
			p.tracer.ThrottlePeer(rpc.from)

		case AcceptAll:
			for _, pmsg := range rpc.GetPublish() {
				if !(p.subscribedToMsg(pmsg) || p.canRelayMsg(pmsg)) {
					log.Debug("received message in topic we didn't subscribe to; ignoring message")
					continue
				}

				p.pushMsg(&Message{pmsg, "", rpc.from, nil, false})
			}
		}

		p.rt.HandleRPC(rpc)
	}, nil)
}

// DefaultMsgIdFn returns a unique ID of the passed Message
func DefaultMsgIdFn(pmsg *pb.Message) string {
	return string(pmsg.GetFrom()) + string(pmsg.GetSeqno())
}

// DefaultPeerFilter accepts all peers on all topics
func DefaultPeerFilter(pid peer.ID, topic string) bool {
	return true
}

// pushMsg pushes a message performing validation as necessary
func (p *PubSub) pushMsg(msg *Message) {
	src := msg.ReceivedFrom
	// reject messages from blacklisted peers
	if p.blacklist.Contains(src) {
		log.Debugf("dropping message from blacklisted peer %s", src)
		p.tracer.RejectMessage(msg, RejectBlacklstedPeer)
		return
	}

	// even if they are forwarded by good peers
	if p.blacklist.Contains(msg.GetFrom()) {
		log.Debugf("dropping message from blacklisted source %s", src)
		p.tracer.RejectMessage(msg, RejectBlacklistedSource)
		return
	}

	err := p.checkSigningPolicy(msg)
	if err != nil {
		log.Debugf("dropping message from %s: %s", src, err)
		return
	}

	// reject messages claiming to be from ourselves but not locally published
	self := p.host.ID()
	if peer.ID(msg.GetFrom()) == self && src != self {
		log.Debugf("dropping message claiming to be from self but forwarded from %s", src)
		p.tracer.RejectMessage(msg, RejectSelfOrigin)
		return
	}

	// have we already seen and validated this message?
	id := p.idGen.ID(msg)
	if p.seenMessage(id) {
		p.tracer.DuplicateMessage(msg)
		return
	}

	if !p.val.Push(src, msg) {
		return
	}

	if p.markSeen(id) {
		p.Mach.Add1(ss.PublishMessage, am.A{"Message": msg})
	}
}

func (p *PubSub) checkSigningPolicy(msg *Message) error {
	// reject unsigned messages when strict before we even process the id
	if p.signPolicy.mustVerify() {
		if p.signPolicy.mustSign() {
			if msg.Signature == nil {
				p.tracer.RejectMessage(msg, RejectMissingSignature)
				return ValidationError{Reason: RejectMissingSignature}
			}
			// Actual signature verification happens in the validation pipeline,
			// after checking if the message was already seen or not,
			// to avoid unnecessary signature verification processing-cost.
		} else {
			if msg.Signature != nil {
				p.tracer.RejectMessage(msg, RejectUnexpectedSignature)
				return ValidationError{Reason: RejectUnexpectedSignature}
			}
			// If we are expecting signed messages, and not authoring messages,
			// then do no accept seq numbers, from data, or key data.
			// The default msgID function still relies on Seqno and From,
			// but is not used if we are not authoring messages ourselves.
			if p.signID == "" {
				if msg.Seqno != nil || msg.From != nil || msg.Key != nil {
					p.tracer.RejectMessage(msg, RejectUnexpectedAuthInfo)
					return ValidationError{Reason: RejectUnexpectedAuthInfo}
				}
			}
		}
	}

	return nil
}

type addTopicReq struct {
	topic *Topic
	resp  chan *Topic
}

type rmTopicReq struct {
	topic *Topic
	resp  chan error
}

type TopicOptions struct{}

type TopicOpt func(t *Topic) error

// WithTopicMessageIdFn sets custom MsgIdFunction for a Topic, enabling topics to have own msg id generation rules.
func WithTopicMessageIdFn(msgId MsgIdFunction) TopicOpt {
	return func(t *Topic) error {
		t.p.idGen.Set(t.topic, msgId)
		return nil
	}
}

// Join joins the topic and returns a Topic handle. Only one Topic handle should exist per topic, and Join will error if
// the Topic handle already exists.
func (p *PubSub) Join(topic string, opts ...TopicOpt) (*Topic, error) {
	t, ok, err := p.tryJoin(topic, opts...)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("topic already exists")
	}

	return t, nil
}

// tryJoin is an internal function that tries to join a topic
// Returns the topic if it can be created or found
// Returns true if the topic was newly created, false otherwise
// Can be removed once pubsub.Publish() and pubsub.Subscribe() are removed
func (p *PubSub) tryJoin(topic string, opts ...TopicOpt) (*Topic, bool, error) {
	if p.subFilter != nil && !p.subFilter.CanSubscribe(topic) {
		return nil, false, fmt.Errorf("topic is not allowed by the subscription filter")
	}

	t := &Topic{
		p:           p,
		topic:       topic,
		evtHandlers: make(map[*TopicEventHandler]struct{}),
	}

	for _, opt := range opts {
		err := opt(t)
		if err != nil {
			return nil, false, err
		}
	}

	resp := make(chan *Topic, 1)
	req := &addTopicReq{
		topic: t,
		resp:  resp,
	}
	p.Mach.Add1(ss.AddTopic, am.A{"addTopicReq": req})
	returnedTopic := <-resp

	if returnedTopic != t {
		return returnedTopic, false, nil
	}

	return t, true, nil
}

type addSubReq struct {
	sub  *Subscription
	resp chan *Subscription
}

type SubOpt func(sub *Subscription) error

// Subscribe returns a new Subscription for the given topic.
// Note that subscription is not an instantaneous operation. It may take some time
// before the subscription is processed by the pubsub main loop and propagated to our peers.
//
// Deprecated: use pubsub.Join() and topic.Subscribe() instead
func (p *PubSub) Subscribe(topic string, opts ...SubOpt) (*Subscription, error) {
	// ignore whether the topic was newly created or not, since either way we have a valid topic to work with
	topicHandle, _, err := p.tryJoin(topic)
	if err != nil {
		return nil, err
	}

	return topicHandle.Subscribe(opts...)
}

// WithBufferSize is a Subscribe option to customize the size of the subscribe output buffer.
// The default length is 32 but it can be configured to avoid dropping messages if the consumer is not reading fast
// enough.
func WithBufferSize(size int) SubOpt {
	return func(sub *Subscription) error {
		sub.ch = make(chan *Message, size)
		return nil
	}
}

type topicReq struct {
	resp chan []string
}

// GetTopics returns the topics this node is subscribed to.
func (p *PubSub) GetTopics() []string {
	req := &topicReq{resp: make(chan []string, 1)}
	p.Mach.Add1(ss.GetTopics, am.A{"topicReq": req})
	select {
	case resp := <-req.resp:
		return resp
	case <-p.ctx.Done():
		return nil
	}
}

// Publish publishes data to the given topic.
//
// Deprecated: use pubsub.Join() and topic.Publish() instead
func (p *PubSub) Publish(topic string, data []byte, opts ...PubOpt) error {
	// ignore whether the topic was newly created or not, since either way we have a valid topic to work with
	t, _, err := p.tryJoin(topic)
	if err != nil {
		return err
	}

	return t.Publish(context.TODO(), data, opts...)
}

func (p *PubSub) nextSeqno() []byte {
	seqno := make([]byte, 8)
	counter := atomic.AddUint64(&p.counter, 1)
	binary.BigEndian.PutUint64(seqno, counter)
	return seqno
}

type listPeerReq struct {
	resp  chan []peer.ID
	topic string
}

// ListPeers returns a list of peers we are connected to in the given topic.
func (p *PubSub) ListPeers(topic string) []peer.ID {
	req := &listPeerReq{
		resp:  make(chan []peer.ID, 1),
		topic: topic,
	}

	p.Mach.Add1(ss.GetPeers, am.A{"listPeerReq": req})
	select {
	case resp := <-req.resp:
		return resp
	case <-p.ctx.Done():
		return nil
	}
}

// BlacklistPeer blacklists a peer; all messages from this peer will be unconditionally dropped.
func (p *PubSub) BlacklistPeer(pid peer.ID) {
	when := p.Mach.WhenArgs(ss.BlacklistPeer, am.A{"pid": pid}, nil)
	p.Mach.Add1(ss.BlacklistPeer, am.A{"pid": pid})
	<-when
}

// RegisterTopicValidator registers a validator for topic.
// By default validators are asynchronous, which means they will run in a separate goroutine.
// The number of active goroutines is controlled by global and per topic validator
// throttles; if it exceeds the throttle threshold, messages will be dropped.
func (p *PubSub) RegisterTopicValidator(topic string, val interface{}, opts ...ValidatorOpt) error {
	req := &addValReq{
		topic:    topic,
		validate: val,
		resp:     make(chan error, 1),
	}

	for _, opt := range opts {
		err := opt(req)
		if err != nil {
			return err
		}
	}

	p.Mach.Add1(ss.AddValidator, am.A{"addValReq": req})
	select {
	case resp := <-req.resp:
		return resp
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// UnregisterTopicValidator removes a validator from a topic.
// Returns an error if there was no validator registered with the topic.
func (p *PubSub) UnregisterTopicValidator(topic string) error {
	req := &rmValReq{
		topic: topic,
		resp:  make(chan error, 1),
	}
	p.Mach.Add1(ss.RemoveValidator, am.A{"rmValReq": req})
	select {
	case resp := <-req.resp:
		return resp
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// SetLogLevelAM changes the machines log level. Used by tests for debugging active nodes.
func (p *PubSub) SetLogLevelAM(level am.LogLevel) {
	p.Mach.SetLogLevel(level)
	p.disc.Mach.SetLogLevel(level)
}

var globHostNum int

// initMachine initializes the pubsub state machine.
func (p *PubSub) initMachine() error {
	var (
		err      error
		machOpts *am.Opts
	)

	// tracing
	if psmon.IsTracingAM() {

		// init the tracer in case only PS is traced
		// TODO psmon.InitOtelTracer()

		// this is the topmost machine, other ones (within this pubsub host) will
		// inherit tracing from this one (but not logging)
		tracer := telemetry.NewOtelMachTracer(psmon.OtelTracer, &telemetry.OtelMachTracerOpts{
			SkipTransitions: !psmon.IsTracingAMTx(),
			Logf:            psmon.Log.Printf,
		})
		psmon.AMTracers = append(psmon.AMTracers, tracer)

		machOpts = &am.Opts{
			Tracers: []am.Tracer{tracer},
		}
	}

	// host number for multi-hosts tests
	hostNum := globHostNum
	globHostNum++
	// pass to submachines
	p.ctx = context.WithValue(p.ctx, "psmonHostNum", hostNum)

	// machine init
	machID := fmt.Sprintf("ps-%d", hostNum)
	p.Mach, err = am.NewCommon(p.ctx, machID, ss.States, ss.Names, p, nil, machOpts)
	if err != nil {
		return err
	}

	// ps monitor init
	psmon.SetUpMach(p.Mach, hostNum)

	if psmon.IsTracingAM() {
		p.Mach.Log("tracing enabled: %d", psmon.IsTracingAMTx())
	}

	return nil
}

type RelayCancelFunc func()

type addRelayReq struct {
	topic string
	resp  chan RelayCancelFunc
}
