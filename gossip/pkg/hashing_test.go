package gossip

import (
	"testing"
)

func TestHash(t *testing.T) {

	testCase := struct{ Key, Value string }{"test", "case"}

	hashed, err := hash(testCase)
	if err != nil {
		t.Fatal(err)
	}

	if len(hashed) == 0 {
		t.Fatal("invalid hash")
	}
}

func TestHashIgnoreField(t *testing.T) {

	t1 := struct {
		Key, Value string
		Hash       string `json:"-"` // should be ignored
	}{Key: "test", Value: "case"}

	hashed, err := hash(t1)
	if err != nil {
		t.Fatal(err)
	}

	t1.Hash = hashed
	hashed2, err := hash(t1)

	if hashed != hashed2 {
		t.Fatalf("hashed values should be the same: found %s and %s", hashed, hashed2)
	}
}
