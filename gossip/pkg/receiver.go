package gossip

// NewReceiver creates a new RPC gossip receiver.
func NewReceiver(store *StateMachine) *Receiver {
	return &Receiver{store: store}
}

// Receiver represents an RPC receiver for the gossip protocol implementation.
type Receiver struct {
	store *StateMachine
}

// Envelope represents a message exchanged during a gossip round.
// The complete local state is exchanged at the beginning of a gossip interaction, though,
// after the envelope is evaluated by the current node, the reply will only contain diffs
// with the received memberships and missing states known by the receiver.
type Envelope struct {
	States []EndpointState
}

// Gossip handles the gossip round request as described above.
func (s *Receiver) Gossip(req *Envelope, reply *Envelope) error {

	locals := s.store.Peers(false)

	// Update cluster memberships in local store and append updates to reply if
	// newer version is known
	reply.States = []EndpointState{}
	for _, state := range req.States {
		newer := s.store.Update(state)
		if newer != nil {
			reply.States = append(reply.States, *newer)
		}
		delete(locals, state.NodeAddr)
	}

	// Add memberships not known by the caller to the reply
	for _, v := range locals {
		reply.States = append(reply.States, v)
	}

	return nil
}
