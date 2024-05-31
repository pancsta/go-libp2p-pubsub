package pubsub

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"
	am "github.com/pancsta/asyncmachine-go/pkg/machine"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"

	pb "github.com/pancsta/go-libp2p-pubsub/pb"
	ss "github.com/pancsta/go-libp2p-pubsub/states/pubsub"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC

	subscriptions := make(map[string]bool)

	for t := range p.mySubs {
		subscriptions[t] = true
	}

	for t := range p.myRelays {
		subscriptions[t] = true
	}

	for t := range subscriptions {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) PeerCloseStreamState(e *am.Event) {
	p.inboundStreamsMx.Lock()
	pid := e.Args["pid"].(peer.ID)
	if s, ok := p.inboundStreams[pid]; ok {
		delete(p.inboundStreams, pid)
		s.Reset()
	}
	p.inboundStreamsMx.Unlock()
}

func (p *PubSub) handleNewStream(s network.Stream) {
	peer := s.Conn().RemotePeer()

	p.inboundStreamsMx.Lock()
	other, dup := p.inboundStreams[peer]
	if dup {
		log.Debugf("duplicate inbound stream from %s; resetting other stream", peer)
		other.Reset()
	}
	p.inboundStreams[peer] = s
	p.inboundStreamsMx.Unlock()

	defer p.Mach.Remove1(ss.PeerCloseStream, am.A{"pid": peer})

	r := msgio.NewVarintReaderSize(s, p.maxMessageSize)
	for p.ctx.Err() == nil {
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err != io.EOF {
				s.Reset()
				log.Debugf("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			} else {
				// Just be nice. They probably won't read this
				// but it doesn't hurt to send it.
				s.Close()
			}

			return
		}
		if len(msgbytes) == 0 {
			continue
		}

		rpc := new(RPC)
		err = rpc.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			s.Reset()
			log.Warnf("bogus rpc from %s: %s", s.Conn().RemotePeer(), err)
			return
		}

		rpc.from = peer
		select {
		case p.incoming <- rpc:
		case <-p.ctx.Done():
			// Close is useless because the other side isn't reading.
			s.Reset()
			return
		}
	}
}

func (p *PubSub) notifyPeerDead(pid peer.ID) {
	p.peerDeadPrioLk.RLock()
	p.peerDeadMx.Lock()
	p.peerDeadPend[pid] = struct{}{}
	p.peerDeadMx.Unlock()
	p.peerDeadPrioLk.RUnlock()
	p.Mach.Add(am.S{ss.PeersDead}, nil)
}

func (p *PubSub) handleNewPeer(ctx context.Context, pid peer.ID, outgoing <-chan *RPC) {
	// TODO fixes "protocol not supported" error, fix the timing issue
	time.Sleep(10 * time.Millisecond)

	s, err := p.host.NewStream(p.ctx, pid, p.rt.Protocols()...)
	if err != nil {
		log.Debug("opening new stream to peer: ", err, pid)
		p.Mach.Add1(ss.PeerError, am.A{"pid": pid})
		return
	}

	go p.handleSendingMessages(ctx, s, outgoing)
	go p.handlePeerDead(s)
	p.Mach.Add1(ss.PeerNewStream, am.A{"network.Stream": s})
}

func (p *PubSub) handleNewPeerWithBackoff(ctx context.Context, pid peer.ID, backoff time.Duration, outgoing <-chan *RPC) {
	select {
	case <-time.After(backoff):
		p.handleNewPeer(ctx, pid, outgoing)
	case <-ctx.Done():
		return
	}
}

func (p *PubSub) handlePeerDead(s network.Stream) {
	pid := s.Conn().RemotePeer()

	_, err := s.Read([]byte{0})
	if err == nil {
		log.Debugf("unexpected message from %s", pid)
	}

	s.Reset()
	p.notifyPeerDead(pid)
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s network.Stream, outgoing <-chan *RPC) {
	writeRpc := func(rpc *RPC) error {
		size := uint64(rpc.Size())

		buf := pool.Get(varint.UvarintSize(size) + int(size))
		defer pool.Put(buf)

		n := binary.PutUvarint(buf, size)
		_, err := rpc.MarshalTo(buf[n:])
		if err != nil {
			return err
		}

		_, err = s.Write(buf)
		return err
	}

	defer s.Close()
	for {
		select {
		case rpc, ok := <-outgoing:
			if !ok {
				return
			}

			err := writeRpc(rpc)
			if err != nil {
				s.Reset()
				log.Debugf("writing message to %s: %s", s.Conn().RemotePeer(), err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Subscriptions: subs,
		},
	}
}

func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{RPC: pb.RPC{Publish: msgs}}
}

func rpcWithControl(msgs []*pb.Message,
	ihave []*pb.ControlIHave,
	iwant []*pb.ControlIWant,
	graft []*pb.ControlGraft,
	prune []*pb.ControlPrune) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Publish: msgs,
			Control: &pb.ControlMessage{
				Ihave: ihave,
				Iwant: iwant,
				Graft: graft,
				Prune: prune,
			},
		},
	}
}

func copyRPC(rpc *RPC) *RPC {
	res := new(RPC)
	*res = *rpc
	if rpc.Control != nil {
		res.Control = new(pb.ControlMessage)
		*res.Control = *rpc.Control
	}
	return res
}
