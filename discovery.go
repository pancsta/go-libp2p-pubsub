package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	discimpl "github.com/libp2p/go-libp2p/p2p/discovery/backoff"
	am "github.com/pancsta/asyncmachine-go/pkg/machine"

	ss "github.com/pancsta/go-libp2p-pubsub/states/discovery"
)

var (
	// poll interval

	// DiscoveryPollInitialDelay is how long the discovery system waits after it first starts before polling
	DiscoveryPollInitialDelay = 0 * time.Millisecond
	// DiscoveryPollInterval is approximately how long the discovery system waits in between checks for whether the
	// more peers are needed for any topic
	DiscoveryPollInterval = 1 * time.Second
)

// interval at which to retry advertisements when they fail.
const discoveryAdvertiseRetryInterval = 2 * time.Minute

type DiscoverOpt func(*discoverOptions) error

type discoverOptions struct {
	connFactory BackoffConnectorFactory
	opts        []discovery.Option
}

func defaultDiscoverOptions() *discoverOptions {
	rngSrc := rand.NewSource(rand.Int63())
	minBackoff, maxBackoff := time.Second*10, time.Hour
	cacheSize := 100
	dialTimeout := time.Minute * 2
	discoverOpts := &discoverOptions{
		connFactory: func(host host.Host) (*discimpl.BackoffConnector, error) {
			backoff := discimpl.NewExponentialBackoff(minBackoff, maxBackoff, discimpl.FullJitter, time.Second, 5.0, 0, rand.New(rngSrc))
			return discimpl.NewBackoffConnector(host, cacheSize, dialTimeout, backoff)
		},
	}

	return discoverOpts
}

// discover represents the discovery pipeline.
// The discovery pipeline handles advertising and discovery of peers
type discover struct {
	p *PubSub

	// discovery assists in discovering and advertising peers for a topic
	discovery discovery.Discovery

	// advertising tracks which topics are being advertised
	advertising map[string]context.CancelFunc

	// discoverQ handles continuing peer discovery
	discoverQ chan *discoverReq

	// ongoing tracks ongoing discovery requests
	ongoing map[string]struct{}

	// done handles completion of a discovery request
	done chan string

	// connector handles connecting to new peers found via discovery
	connector *discimpl.BackoffConnector

	// options are the set of options to be used to complete struct construction in Start
	options *discoverOptions

	mach *am.Machine

	ongoingBootstrap     map[string]*bootstrapFlow
	ongoingBootstrapLock sync.Mutex
}

// MinTopicSize returns a function that checks if a router is ready for publishing based on the topic size.
// The router ultimately decides the whether it is ready or not, the given size is just a suggestion. Note
// that the topic size does not include the router in the count.
func MinTopicSize(size int) RouterReady {
	return func(rt PubSubRouter, topic string) (bool, error) {
		return rt.EnoughPeers(topic, size), nil
	}
}

// Start attaches the discovery pipeline to a pubsub instance, initializes discovery and starts event loop
func (d *discover) Start(p *PubSub, opts ...DiscoverOpt) error {
	if d.discovery == nil || p == nil {
		return nil
	}
	var err error

	d.p = p
	d.advertising = make(map[string]context.CancelFunc)
	d.ongoing = make(map[string]struct{})
	d.ongoingBootstrap = make(map[string]*bootstrapFlow)

	hostNum := p.ctx.Value("psmonHostNum").(int)
	machName := fmt.Sprintf("ps-%d-disc", hostNum)
	d.mach, err = am.NewCommon(d.p.Mach.Ctx, machName,
		ss.States, ss.Names, d, d.p.Mach, nil)
	if err != nil {
		return err
	}
	psmon.SetUpMach(d.mach, hostNum)

	conn, err := d.options.connFactory(p.host)
	if err != nil {
		return err
	}
	d.connector = conn

	if res := d.mach.Add1(ss.Start, nil); res == am.Canceled {
		return am.ErrCanceled
	}

	return nil
}

func (d *discover) PoolTimerState(e *am.Event) {
	stateCtx := e.Machine.NewStateCtx(ss.PoolTimer)

	go func() {
		select {
		case <-stateCtx.Done():
			return
		case <-time.After(DiscoveryPollInitialDelay):
		}
		d.mach.Add1(ss.RefreshingDiscoveries, nil)

		if DiscoveryPollInterval == 0 {
			return
		}
		ticker := time.NewTicker(DiscoveryPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-d.mach.Ctx.Done():
				return
			case <-ticker.C:
				d.mach.Add1(ss.RefreshingDiscoveries, nil)
			}
		}
	}()
}

func (d *discover) RefreshingDiscoveriesState(e *am.Event) {
	d.mach.Remove1(ss.RefreshingDiscoveries, nil)
	for t := range d.p.myTopics {
		if !d.p.rt.EnoughPeers(t, 0) {
			req := &discoverReq{topic: t}
			d.mach.Add1(ss.DiscoveringTopic, am.A{
				"discoverReq": req,
				"topic":       t,
				"source":      "discover.RefreshingDiscoveriesState",
			})
		}
	}
}

func (d *discover) DiscoveringTopicExit(e *am.Event) bool {
	return len(d.ongoing) == 0
}

func (d *discover) DiscoveringTopicState(e *am.Event) {
	req := e.Args["discoverReq"].(*discoverReq)
	topic := req.topic

	if _, ok := d.ongoing[topic]; ok {
		return
	}

	d.ongoing[topic] = struct{}{}
	ctx := d.mach.Ctx

	// write the map early
	delete(d.ongoing, topic)

	go func() {
		discoverCtx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		peerCh, err := d.discovery.FindPeers(discoverCtx, topic, req.opts...)
		if err != nil {
			log.Debugf("error finding peers for topic %s: %v", topic, err)
			delete(d.ongoing, topic)
			return
		}

		// TODO err not handled from `c.host.Connect(ctx, pi)`
		d.connector.Connect(ctx, peerCh)
		d.mach.Remove1(ss.DiscoveringTopic, nil)
		d.mach.Add1(ss.TopicDiscovered, am.A{"topic": topic})
	}()
}

func (d *discover) AdvertisingTopicExit(_ *am.Event) bool {
	return len(d.advertising) == 0
}

func (d *discover) AdvertisingTopicState(e *am.Event) {
	topic := e.Args["topic"].(string)

	if _, ok := d.advertising[topic]; ok {
		// in progress
		d.mach.Log("already advertising topic %s", topic)
		return
	}

	advertisingCtx, cancel := context.WithCancel(d.mach.Ctx)
	d.advertising[topic] = cancel

	go func() {
		next, err := d.discovery.Advertise(advertisingCtx, topic)
		if err != nil {
			log.Warnf("bootstrap: error providing rendezvous for %s: %s", topic, err.Error())
			if next == 0 {
				next = discoveryAdvertiseRetryInterval
			}
		}

		t := time.NewTimer(next)
		defer t.Stop()

		for advertisingCtx.Err() == nil {
			select {
			case <-t.C:
				next, err = d.discovery.Advertise(advertisingCtx, topic)
				if err != nil {
					log.Warnf("bootstrap: error providing rendezvous for %s: %s", topic, err.Error())
					if next == 0 {
						next = discoveryAdvertiseRetryInterval
					}
				}
				t.Reset(next)
			case <-advertisingCtx.Done():
				return
			}
		}
	}()
}

// Advertise advertises this node's interest in a topic to a discovery service.
func (d *discover) Advertise(topic string) {
	if d.discovery == nil {
		return
	}
	d.mach.Add1(ss.AdvertisingTopic, am.A{"topic": topic})
}

func (d *discover) StopAdvertisingTopicState(e *am.Event) {
	defer d.mach.Remove1(ss.StopAdvertisingTopic, nil)
	topic := e.Args["topic"].(string)

	if advertiseCancel, ok := d.advertising[topic]; ok {
		advertiseCancel()
		delete(d.advertising, topic)
	}

	d.mach.Remove1(ss.AdvertisingTopic, nil)
}

// StopAdvertise stops advertising this node's interest in a topic. StopAdvertise is not thread-safe.
func (d *discover) StopAdvertise(topic string) {
	if d.discovery == nil {
		return
	}

	d.mach.Add1(ss.StopAdvertisingTopic, am.A{"topic": topic})
}

// Discover searches for additional peers interested in a given topic
func (d *discover) Discover(topic string, opts ...discovery.Option) {
	if d.discovery == nil {
		return
	}

	d.mach.Add1(ss.DiscoveringTopic, am.A{
		"discoverReq": &discoverReq{topic, opts},
		"source":      "discover.Discover",
	})
}

func (d *discover) BootstrappingTopicEnter(e *am.Event) bool {
	topic := e.Args["topic"].(string)
	if _, ok := d.ongoingBootstrap[topic]; ok {
		return false
	}
	return true
}

func (d *discover) BootstrappingTopicState(e *am.Event) {
	topic := e.Args["topic"].(string)
	ctx := e.Args["ctx"].(context.Context)
	opts := e.Args["opts"].([]discovery.Option)
	ready := e.Args["ready"].(RouterReady)

	// init a BootstrapFlow machine for this topic
	flow, err := newBootstrapFlow(ctx, d, topic, ready, opts)
	if err != nil {
		d.mach.AddErr(err)
	}
	d.mach.Log("created a BootstrapFlow for topic %s", topic)

	// grouping multi states need locks
	d.ongoingBootstrapLock.Lock()
	d.ongoingBootstrap[topic] = flow
	d.ongoingBootstrapLock.Unlock()

	// start the bootstrapping flow
	flow.mach.Add1(ss.Start, nil)
	go func() {

		d.mach.Log("BootstrappingTopicState before")
		select {
		case <-d.mach.Ctx.Done():
			return
		case <-flow.mach.When1(ss.TopicBootstrapped, nil):
		}
		d.mach.Log("BootstrappingTopicState done")
		d.mach.Add1(ss.TopicBootstrapped, am.A{"topic": topic})
		d.mach.Remove1(ss.BootstrappingTopic, am.A{"topic": topic})

		// clean up
		d.ongoingBootstrapLock.Lock()
		delete(d.ongoingBootstrap, topic)
		d.ongoingBootstrapLock.Unlock()
		flow.mach.Dispose()
	}()
}

func (d *discover) BootstrappingTopicExit(e *am.Event) bool {
	d.ongoingBootstrapLock.Lock()
	defer d.ongoingBootstrapLock.Unlock()
	return len(d.ongoingBootstrap) == 0
}

func (d *discover) TopicBootstrappedState(e *am.Event) {
	topic := e.Args["topic"].(string)
	delete(d.ongoingBootstrap, topic)
	d.mach.Remove1(ss.BootstrappingTopic, nil)
}

// Bootstrap attempts to bootstrap to a given topic. Returns true if bootstrapped successfully, false otherwise.
func (d *discover) Bootstrap(ctx context.Context, topic string, ready RouterReady, opts ...discovery.Option) bool {
	if d.discovery == nil {
		return true
	}
	d.mach.Log("Bootstrap")

	// match state with args (has to be done before mach.Add)
	whenBootstrapped := d.mach.WhenArgs(ss.TopicBootstrapped, am.A{"topic": topic}, nil)
	d.mach.Add1(ss.BootstrappingTopic,
		am.A{"ctx": ctx, "topic": topic, "opts": opts, "ready": ready})

	select {
	case <-whenBootstrapped:
		return true
	case <-d.mach.Ctx.Done():
		return false
	case <-ctx.Done():
		return false
	}
}

type discoverReq struct {
	topic string
	opts  []discovery.Option
}

type pubSubDiscovery struct {
	discovery.Discovery
	opts []discovery.Option
}

func (d *pubSubDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	return d.Discovery.Advertise(ctx, "floodsub:"+ns, append(opts, d.opts...)...)
}

func (d *pubSubDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	return d.Discovery.FindPeers(ctx, "floodsub:"+ns, append(opts, d.opts...)...)
}

// WithDiscoveryOpts passes libp2p Discovery options into the PubSub discovery subsystem
func WithDiscoveryOpts(opts ...discovery.Option) DiscoverOpt {
	return func(d *discoverOptions) error {
		d.opts = opts
		return nil
	}
}

// BackoffConnectorFactory creates a BackoffConnector that is attached to a given host
type BackoffConnectorFactory func(host host.Host) (*discimpl.BackoffConnector, error)

// WithDiscoverConnector adds a custom connector that deals with how the discovery subsystem connects to peers
func WithDiscoverConnector(connFactory BackoffConnectorFactory) DiscoverOpt {
	return func(d *discoverOptions) error {
		d.connFactory = connFactory
		return nil
	}
}

///////////////
///// BOOTSTRAP FLOW
///////////////

// bootstrapFlow is a state machine that handles the bootstrapping process for a given topic.
type bootstrapFlow struct {
	am.ExceptionHandler

	ctx        context.Context
	d          *discover
	mach       *am.Machine
	topic      string
	checkReady RouterReady
	opts       []discovery.Option
	timer      *time.Timer
}

func newBootstrapFlow(ctx context.Context, d *discover, topic string, ready RouterReady, opts []discovery.Option) (*bootstrapFlow, error) {

	b := &bootstrapFlow{
		ctx:        ctx,
		d:          d,
		topic:      topic,
		checkReady: ready,
		opts:       opts,
	}
	hostNum := d.p.ctx.Value("psmonHostNum").(int)
	tick := d.mach.Clock(ss.BootstrappingTopic)

	id := fmt.Sprintf("ps-%d-disc-bf-%d-%s", hostNum, tick, topic)
	mach, err := am.NewCommon(d.mach.Ctx, id, ss.StatesBootstrapFlow, ss.NamesBootstrapFlow, b, d.mach, nil)
	if err != nil {
		return nil, err
	}

	psmon.SetUpMach(mach, hostNum)
	b.mach = mach

	return b, nil
}

func (b *bootstrapFlow) StartState(e *am.Event) {
	b.timer = time.NewTimer(time.Hour)
}

func (b *bootstrapFlow) StartEnd(e *am.Event) {
	b.timer.Stop()
}

func (b *bootstrapFlow) BootstrapCheckingState(e *am.Event) {

	// Check if ready for publishing
	ps := b.d.p
	var ready bool
	readyFn := func() {
		// TODO make sure checkReady doesnt block
		ready, _ = b.checkReady(ps.rt, b.topic)
		b.mach.Log("checkReady for %s: %v", b.mach.ID, ready)
	}

	// run on the main pubsubs queue
	ps.Mach.Eval(nil, readyFn, "bootstrapFlow.BootstrapCheckingState")
	if ready {
		b.mach.Add1(ss.TopicBootstrapped, nil)
		return
	}

	// If not ready discover more peers
	b.mach.Add1(ss.DiscoveringTopic, am.A{
		"source": "bootstrapFlow.BootstrapCheckingState",
	})
}

func (b *bootstrapFlow) DiscoveringTopicState(e *am.Event) {

	args := am.A{"topic": b.topic}
	whenDiscovered := b.d.mach.WhenArgs(ss.TopicDiscovered, args, nil)
	b.d.mach.Add1(ss.DiscoveringTopic, am.A{
		"discoverReq": &discoverReq{b.topic, b.opts},
		"source":      "bootstrapFlow.DiscoveringTopicState",
	})

	stateCtx := b.mach.NewStateCtx(ss.DiscoveringTopic)
	go func() {
		if stateCtx.Err() != nil {
			return // expired
		}

		select {
		case <-b.ctx.Done():
		case <-b.mach.WhenErr(nil):
		case <-stateCtx.Done():
		case <-whenDiscovered:
			b.mach.Add1(ss.BootstrapDelay, nil)
		}
	}()
}

func (b *bootstrapFlow) BootstrapDelayState(e *am.Event) {

	stateCtx := b.mach.NewStateCtx(ss.BootstrapDelay)
	go func() {
		b.timer.Reset(time.Millisecond * 100)
		if stateCtx.Err() != nil {
			return // expired
		}

		select {
		case <-b.ctx.Done():
		case <-b.mach.WhenErr(nil):
		case <-stateCtx.Done():
		case <-b.timer.C:
			b.mach.Add1(ss.BootstrapChecking, nil)
		}
	}()
}
