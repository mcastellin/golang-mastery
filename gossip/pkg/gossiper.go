// Package gossip package contains a toy implementation of the gossip protocol to maintain
// cluster membership information of a set of distributed nodes.
package gossip

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	// The number of gossip peers to interact with on every gossip round.
	numGossipRoundPeers = 2
	// Interval  between each gossip round.
	gossipRoundInterval = 800 * time.Millisecond
	// Interval between heart beats.
	heartBeatInterval = time.Second
	// Registered name of the gossip receiver
	gossipReceiverRPC = "GossReceiver"
)

// NewGossiper creates a new Gossiper.
func NewGossiper(bind string, seed bool, seedAddrs []string) *Gossiper {
	store := &StateMachine{store: map[NodeAddr]EndpointState{}}

	engine := rpc.NewServer()
	rcvr := NewReceiver(store)
	engine.RegisterName(gossipReceiverRPC, rcvr)
	return &Gossiper{
		BindAddr:      bind,
		IsSeed:        seed,
		SeedDialAddrs: seedAddrs,
		Generation:    uint64(time.Now().UnixNano() / 1000),
		closing:       make(chan chan error),
		engine:        engine,
		store:         store,
	}
}

// Gossiper is a naive implementation of the Gossip protocol (https://en.wikipedia.org/wiki/Gossip_protocol)
// that uses epidemic-style communication to share cluster membership information.
//
// The key points of this implementation are the following:
//   - cluster membership states are maintained into an in-memory data structure for every node. Every node
//     is completely oblivious of the real state of the cluster and its knowledge is limited to the content
//     of its internal state
//   - on every gossip round, the node exchanges its entire internal state with randomly selected peers. Peers
//     receiving gossip requests are responsible for comparing the received state with their own stored state
//     and reply with any information that is either missing or more recent than the one received in the request.
//   - every node is responsible for maintaining and sharing its own heart beat. Key components of heartbeats are
//     the Generation number (which is updated on every server restart) and a Version number that increases on every
//     beat.
type Gossiper struct {
	BindAddr      string
	IsSeed        bool
	SeedDialAddrs []string
	Generation    uint64

	Port int

	closing    chan chan error
	engine     *rpc.Server
	store      *StateMachine
	shutdown   bool
	muShutdown sync.RWMutex
}

// Serve the Gossiper RPC (Remote Procedure Call) endpoint and spawn subroutines that handle gossip rounds and heart beats.
func (s *Gossiper) Serve() error {
	s.initState()

	s.muShutdown.Lock()
	s.shutdown = false
	s.muShutdown.Unlock()

	l, err := net.Listen("tcp", s.BindAddr)
	if err != nil {
		return err
	}
	s.Port = l.Addr().(*net.TCPAddr).Port

	ctx, cancel := context.WithCancel(context.Background())
	go s.serveLoop(l, cancel)
	go s.heartBeatLoop(ctx)
	go s.gossipRound(ctx)

	return nil
}

// Shutdown the Gossiper RPC (Remote Procedure Call) service by sending termination signals to goroutines
// and waiting for acknowledgment.
func (s *Gossiper) Shutdown() error {
	s.muShutdown.RLock()
	shutdown := s.shutdown
	s.muShutdown.RUnlock()

	if !shutdown {
		s.muShutdown.Lock()
		s.shutdown = true
		s.muShutdown.Unlock()

		errch := make(chan error)
		s.closing <- errch
		return <-errch
	}
	return fmt.Errorf("server already shutdown")
}

// Nodes returns the current local view of cluster memberships.
func (s *Gossiper) Nodes() []NodeAddr {
	onlinePeers := s.store.Peers(true)
	nodes := make([]NodeAddr, len(onlinePeers))
	idx := 0
	for addr := range onlinePeers {
		nodes[idx] = addr
		idx++
	}
	return nodes
}

// initState initializes the internal storage with knowledge of the node itself with its current version,
// plus knowledge of the seed nodes as available peers. Seed nodes are initialized with both Generation and
// Version = 0 to indicate that we don't know anything about these nodes yet other than they exist.
func (s *Gossiper) initState() {
	selfAddr := NodeAddr(s.BindAddr)
	states := []EndpointState{{
		NodeAddr:  selfAddr,
		HeartBeat: HeartBeatState{Generation: s.Generation, Version: 0},
	}}
	for _, seed := range s.SeedDialAddrs {
		states = append(states, EndpointState{
			NodeAddr:  NodeAddr(seed),
			HeartBeat: HeartBeatState{Generation: 0, Version: 0},
		})
	}
	for _, state := range states {
		s.store.Update(state)
	}

	s.store.Beat(selfAddr)
}

// heartBeatLoop is responsible for heart-beating of the node itself at a regular intervals.
// Termination is handled with a simple context cancellation as we don't need to report errors.
func (s *Gossiper) heartBeatLoop(ctx context.Context) {
	for {
		select {
		case <-time.After(heartBeatInterval):
			s.store.Beat(NodeAddr(s.BindAddr))
		case <-ctx.Done():
			return
		}
	}
}

// gossipRound periodically exchanges information about the cluster state with randomly selected peers.
// Gossip interactions with other peers exchange information via Remote Procedure Calls.
func (s *Gossiper) gossipRound(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(gossipRoundInterval):
			selfAddr := NodeAddr(s.BindAddr)
			gossPeers := s.store.RandomPeers(numGossipRoundPeers, []NodeAddr{selfAddr})
			if len(gossPeers) <= 0 {
				fmt.Println(s.BindAddr, "all alone in this cluster")
				continue
			}

			for _, peer := range gossPeers {

				client, err := rpc.Dial("tcp", string(peer))
				if err != nil {
					fmt.Println(err.Error())
					s.store.Taint(peer)
					continue
				}

				peers := s.store.Peers(false)
				states := make([]EndpointState, len(peers))
				i := 0
				for _, v := range peers {
					states[i] = v
					i++
				}

				var once sync.Once
				req := Envelope{States: states}
				var reply Envelope

				serviceMethod := fmt.Sprintf("%s.Gossip", gossipReceiverRPC)
				if err := client.Call(serviceMethod, &req, &reply); err != nil {
					fmt.Println(err.Error())
					once.Do(func() { client.Close() })
					continue
				}

				// Updating local states from the envelope
				for _, state := range reply.States {
					s.store.Update(state)
				}
				once.Do(func() { client.Close() })
			}
		}
	}
}

// serveLoop is the goroutine responsible for handling incoming RPC calls.
// The loop is implemented using channels for inter-process communication. Accepting and serving
// requests are handled by two separate cases and in its own goroutine to allow for immediate
// processing of graceful shutdown requests.
func (s *Gossiper) serveLoop(l net.Listener, cancel context.CancelFunc) {
	defer l.Close()
	defer cancel()

	serving := make(chan net.Conn, 1)
	accepting := make(chan struct{}, 1)
	accepting <- struct{}{} //initiate the loop
	for {
		select {
		case <-accepting:
			go func() {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				serving <- conn
			}()

		case conn, ok := <-serving:
			if !ok {
				// channel closed
				return
			}
			go s.engine.ServeConn(conn)
			accepting <- struct{}{}

		case errch := <-s.closing:
			errch <- nil
			return
		}
	}
}
