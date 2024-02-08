package plugin

import (
	"fmt"
	"testing"
)

type mockRPCService struct {
	Prefix string
}

func (s *mockRPCService) Echo(input *string, reply *string) error {
	*reply = fmt.Sprintf("%s-%s", s.Prefix, *input)
	return nil
}

func TestPluginRPC(t *testing.T) {
	numCalls := 100
	tests := make([]string, numCalls)
	for i := 0; i < numCalls; i++ {
		tests[i] = fmt.Sprintf("test_%d", i)
	}

	server := &Server{}
	server.Register("fooEcho", &mockRPCService{Prefix: "foo"})
	server.Register("barEcho", &mockRPCService{Prefix: "bar"})

	port, err := server.Serve()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown()

	client := &Client{DialAddr: fmt.Sprintf(":%d", port)}

	for i, test := range tests {
		var svc string
		if i%2 != 0 {
			svc = "foo"
		} else {
			svc = "bar"
		}

		var reply string
		expected := fmt.Sprintf("%s-%s", svc, test)

		serviceName := fmt.Sprintf("%sEcho.Echo", svc)
		client.Call(serviceName, &test, &reply)
		if expected != reply {
			t.Fatalf("plugin call failed: expected %s, found %s", expected, reply)
		}
	}
}
