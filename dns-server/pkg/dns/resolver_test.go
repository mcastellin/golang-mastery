package dns

import (
	"slices"
	"strings"
	"testing"
)

type MockForwarder struct {
	NumCalled int
}

func (ff *MockForwarder) Forward(req []byte) ([]byte, error) {
	ff.NumCalled++
	return []byte{}, nil
}

func TestShouldForwardRequest(t *testing.T) {

	mockFwd := &MockForwarder{}
	resolver := &DNSResolver{
		Fwd:     mockFwd,
		Records: DNSLocalStore{},
	}

	req := getTestDNSRequest()

	_, err := resolver.Resolve(req.Serialize())
	if err != nil {
		t.Fatalf("%v", err)
	}
	if mockFwd.NumCalled != 1 {
		t.Fatalf("expected %d forwards, found %d", 1, mockFwd.NumCalled)
	}
}

func TestShouldNotForwardIfNotDesired(t *testing.T) {

	mockFwd := &MockForwarder{}
	resolver := &DNSResolver{
		Fwd:     mockFwd,
		Records: DNSLocalStore{},
	}

	req := getTestDNSRequest()
	req.RD = false // no recursion
	_, err := resolver.Resolve(req.Serialize())
	if err != nil {
		t.Fatalf("%v", err)
	}
	if mockFwd.NumCalled != 0 {
		t.Fatalf("expected %d forwards, found %d", 0, mockFwd.NumCalled)
	}
}

func TestShouldReplyFromLocalStorage(t *testing.T) {

	store := &DNSLocalStore{}
	store.handleFromFile(strings.NewReader(`; comment
example.com.  127.0.0.1`))

	mockFwd := &MockForwarder{}
	resolver := &DNSResolver{
		Fwd:     mockFwd,
		Records: *store,
	}

	req := getTestDNSRequest()
	bytes, err := resolver.Resolve(req.Serialize())
	if err != nil {
		t.Fatalf("%v", err)
	}
	if mockFwd.NumCalled != 0 {
		t.Fatalf("expected %d forwards, found %d", 0, mockFwd.NumCalled)
	}

	reply := &DNS{}
	err = reply.Decode(bytes)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(reply.Answers) != 1 {
		t.Fatalf("expected %d answers, found %d", 1, len(reply.Answers))
	}
	an := reply.Answers[0]
	if string(an.Name) != "example.com." {
		t.Fatalf("expected answer for name %s, found %s", "example.com.", string(an.Name))
	}
	if !slices.Equal(an.IP, []byte{127, 0, 0, 1}) {
		t.Fatalf("expected answer with IP addr %s, found %v", "127.0.0.1", an.IP)
	}
}

func getTestDNSRequest() *DNS {
	req := &DNS{}
	req.ID = 1
	req.QR = false
	req.RD = true
	req.QDCount = uint16(1)
	req.Questions = []DNSQuestion{
		{
			Name:  []byte("example.com."),
			Type:  DNSTypeA,
			Class: DNSClassIN,
		},
	}
	return req
}
