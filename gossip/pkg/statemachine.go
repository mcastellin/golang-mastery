package gossip

import (
	"slices"
	"sync"
)

// taintedThreshold represents the number of taints received for a certain NodeAddr
// after which we consider the node to be inactive.
const taintedThreshold = 3

// NodeAddr represents a cluster node tcp dial address.
type NodeAddr string

// EndpointState represents the state of a node's membership in the current cluster.
type EndpointState struct {
	NodeAddr  NodeAddr
	HeartBeat HeartBeatState
}

// HeartBeatState represents the heartbeat of a node.
// Every node restart will assign an ever-increasing Generation number so, in case of node
// restart we can recognize messages from a new Generation will supersede older, tainted heartbeats.
type HeartBeatState struct {
	Generation, Version, Tainted uint64
}

// Active tells whether a node is active or not.
// A HeartBeatState is marked as inactive when the number of taints received is bigger than the taintedThreshold.
func (hb *HeartBeatState) Active() bool {
	return hb.Tainted < taintedThreshold
}

// NewStateMachine creates a new StateMachine object to hold node membership information for the cluster.
func NewStateMachine() *StateMachine {
	return &StateMachine{store: map[NodeAddr]EndpointState{}}
}

// StateMachine is an internal type that wraps node membership information for the cluster.
type StateMachine struct {
	mu    sync.RWMutex
	store map[NodeAddr]EndpointState
}

// Peers returns the list of EndpointStates found in local storage.
// When the onlineOnly flag is true, this function only returns the list of active
// peers, the ones that have not been tainted after several broken connection attempts.
func (s *StateMachine) Peers(onlineOnly bool) map[NodeAddr]EndpointState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := map[NodeAddr]EndpointState{}
	for k, v := range s.store {
		if onlineOnly {
			if v.HeartBeat.Active() {
				out[k] = v
			}
		} else {
			out[k] = v
		}
	}
	return out
}

// RandomPeers returns a randomized list of known peers NodeAddr.
// This function is used by the gossiper server to randomize the list of peers
// to gossip with on every round.
func (s *StateMachine) RandomPeers(num int, exclude []NodeAddr) []NodeAddr {
	allPeers := s.Peers(true)

	s.mu.RLock()
	defer s.mu.RUnlock()

	validPeers := []NodeAddr{}
	for addr := range allPeers {
		if !slices.Contains(exclude, addr) {
			validPeers = append(validPeers, addr)
		}
	}

	indexes := randIndexes(len(validPeers), num)

	out := make([]NodeAddr, len(indexes))
	for i, idx := range indexes {
		out[i] = validPeers[idx]
	}
	return out
}

// Beat Version number of the specified NodeAddr.
// This function is solely useful to the Gossiper itself to increase its own heartbeats and
// reset Taint values.
func (s *StateMachine) Beat(node NodeAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, exists := s.store[node]
	if !exists {
		return
	}
	elem.HeartBeat.Version++
	elem.HeartBeat.Tainted = 0
	s.store[node] = elem
}

// Taint the cluster membership for node with the specified NodeAddr.
// "Tainting" will effectively increase the taint counter for the node's HeartBeat and
// Version is incremented so the information will be shared in the next gossip round.
func (s *StateMachine) Taint(node NodeAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, exists := s.store[node]
	if !exists {
		return
	}
	elem.HeartBeat.Version++
	elem.HeartBeat.Tainted++
	s.store[node] = elem
}

// Update cluster membership information in local storage.
// This function only updates the local storage if the state received as a parameter
// is more recent than what the current node has. Data freshness is validated using
// Generation and Version numbers.
// If the local store contains more up-to-date information about the EndpointState,
// the entire state value is returned so it can be shared with the initiator of the
// gossip round.
func (s *StateMachine) Update(state EndpointState) *EndpointState {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := state.NodeAddr

	elem, exists := s.store[key]
	if !exists {
		s.store[key] = state
		return nil
	}

	switch {
	case elem.HeartBeat.Generation > state.HeartBeat.Generation:
		// I have a newer generation entirely, hence returning the updated state
		out := elem
		return &out
	case elem.HeartBeat.Generation < state.HeartBeat.Generation:
		// I have an old generation. Updating mine
		s.store[key] = state
		return nil
	}
	if elem.HeartBeat.Version <= state.HeartBeat.Version {
		s.store[key] = state
		return nil
	}
	out := elem
	return &out
}
