package pubsub

import (
	"context"
	"sync"

	ss "github.com/pancsta/go-libp2p-pubsub/states/pubsub"
	am "github.com/pancsta/asyncmachine-go/pkg/machine"
)

// Subscription handles the details of a particular Topic subscription.
// There may be many subscriptions for a given Topic.
type Subscription struct {
	topic  string
	ch     chan *Message
	psMach *am.Machine
	ctx    context.Context
	err    error
	once   sync.Once
	//mach     *am.Machine
}

// Topic returns the topic string associated with the Subscription
func (sub *Subscription) Topic() string {
	return sub.topic
}

// Next returns the next message in our subscription
func (sub *Subscription) Next(ctx context.Context) (*Message, error) {
	select {
	case msg, ok := <-sub.ch:
		if !ok {
			return msg, sub.err
		}

		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel closes the subscription. If this is the last active subscription then pubsub will send an unsubscribe
// announcement to the network.
func (sub *Subscription) Cancel() {
	sub.psMach.Add1(ss.RemoveSubscription, am.A{"Subscription": sub})
}

func (sub *Subscription) close() {
	sub.once.Do(func() {
		close(sub.ch)
	})
}
