package gossip

import (
	"testing"
)

func initTestStore(initial []EndpointState) *StateMachine {
	store := NewStateMachine()
	for _, v := range initial {
		store.Update(v)
	}

	return store
}

func statesEquals(a, b EndpointState) bool {
	return a.NodeAddr == b.NodeAddr &&
		a.HeartBeat.Generation == b.HeartBeat.Generation &&
		a.HeartBeat.Version == b.HeartBeat.Version
}

func TestUpdate(t *testing.T) {
	testCases := []struct {
		Update              EndpointState
		ExpectedReturn      bool
		ExpectedStateUpdate bool
	}{
		{
			Update: EndpointState{
				NodeAddr:  "test",
				HeartBeat: HeartBeatState{Generation: 1, Version: 9999},
			},
			ExpectedReturn:      true,
			ExpectedStateUpdate: false,
		}, {
			Update: EndpointState{
				NodeAddr:  "test",
				HeartBeat: HeartBeatState{Generation: 1234, Version: 10},
			},
			ExpectedReturn:      true,
			ExpectedStateUpdate: false,
		}, {
			Update: EndpointState{
				NodeAddr:  "test",
				HeartBeat: HeartBeatState{Generation: 1234, Version: 9999},
			},
			ExpectedReturn:      false,
			ExpectedStateUpdate: true,
		},
	}

	initial := []EndpointState{{
		NodeAddr:  "test",
		HeartBeat: HeartBeatState{Generation: 1234, Version: 1234},
	}}
	for _, test := range testCases {
		store := initTestStore(initial)
		result := store.Update(test.Update)
		if test.ExpectedReturn && result == nil {
			t.Fatalf("update %v expected a return but found nil", test)
		}

		current := store.Peers(false)[test.Update.NodeAddr]
		if test.ExpectedStateUpdate && !statesEquals(current, test.Update) {
			t.Fatalf("case %v should have updated state. Found %v", test.Update, current)
		}
		if !test.ExpectedStateUpdate && statesEquals(current, test.Update) {
			t.Fatalf("case %v should NOT have updated state. Found %v", test.Update, current)
		}
	}
}
