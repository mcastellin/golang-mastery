package gossip

import "testing"

func TestReceiver(t *testing.T) {
	receiverState := []EndpointState{
		{
			NodeAddr:  "localhost:8080",
			HeartBeat: HeartBeatState{Generation: 1, Version: 1234},
		},
		{
			NodeAddr:  "localhost:8081",
			HeartBeat: HeartBeatState{Generation: 10, Version: 3},
		},
	}

	gossip := []EndpointState{
		{
			NodeAddr:  "localhost:8081",
			HeartBeat: HeartBeatState{Generation: 10, Version: 6},
		},
		{
			NodeAddr:  "localhost:8082",
			HeartBeat: HeartBeatState{Generation: 99, Version: 2},
		},
	}

	store := initTestStore(receiverState)

	rcvr := NewReceiver(store)

	req := Envelope{States: gossip}
	var reply Envelope
	err := rcvr.Gossip(&req, &reply)

	if err != nil {
		t.Fatal(err)
	}

	// check reply has unknonw state
	found := false
	for _, v := range reply.States {
		if v.NodeAddr == NodeAddr("localhost:8080") {
			found = true
		}
	}

	if !found {
		t.Fatal("gossip reply is missing status known to receiver")
	}

	peers := store.Peers(false)
	if _, ok := peers[NodeAddr("localhost:8082")]; !ok {
		t.Fatal("store is missing state information for localhost:8082")
	}

	if peers[NodeAddr("localhost:8081")].HeartBeat.Version != 6 {
		t.Fatal("store information was not updated")
	}
}
