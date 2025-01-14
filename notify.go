package pubsub

import (
	ss "github.com/pancsta/go-libp2p-pubsub/states/pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	am "github.com/pancsta/asyncmachine-go/pkg/machine"
)

var _ network.Notifiee = (*PubSubNotif)(nil)

type PubSubNotif PubSub

func (p *PubSubNotif) OpenedStream(n network.Network, s network.Stream) {
}

func (p *PubSubNotif) ClosedStream(n network.Network, s network.Stream) {
}

func (p *PubSubNotif) Connected(n network.Network, c network.Conn) {
	// ignore transient connections
	if c.Stat().Limited {
		return
	}

	go func() {
		p.newPeersPrioLk.RLock()
		p.newPeersMx.Lock()
		p.newPeersPend[c.RemotePeer()] = struct{}{}
		p.newPeersMx.Unlock()
		p.newPeersPrioLk.RUnlock()

		p.Mach.Add(am.S{ss.PeersPending}, nil)
	}()
}

func (p *PubSubNotif) Disconnected(n network.Network, c network.Conn) {
}

func (p *PubSubNotif) Listen(n network.Network, _ ma.Multiaddr) {
}

func (p *PubSubNotif) ListenClose(n network.Network, _ ma.Multiaddr) {
}

func (p *PubSubNotif) Initialize() {
	isTransient := func(pid peer.ID) bool {
		for _, c := range p.host.Network().ConnsToPeer(pid) {
			if !c.Stat().Limited {
				return false
			}
		}

		return true
	}

	p.newPeersPrioLk.RLock()
	p.newPeersMx.Lock()
	for _, pid := range p.host.Network().Peers() {
		if isTransient(pid) {
			continue
		}

		p.newPeersPend[pid] = struct{}{}
	}
	p.newPeersMx.Unlock()
	p.newPeersPrioLk.RUnlock()

	p.Mach.Add1(ss.PeersPending, nil)
}
