// Package plugin contains an RPC client and server implementation
// used to enable the communication between a main program and a set of
// third party plugins.
//
// Why use RPC for plugin communication?
// Even though RPC communication is slower than embedding plugins directly into the codebase,
// this strategy further decouples the program from plugins logic, to the point that,
// if needed for security reasons, the two components could be separated and ran as
// completely separate subprocess or remote plugin server.
package plugin

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// Client struct represents an RPC client to allow plugin communication.
type Client struct {

	// DialAddr is the network address of the running plugin server.
	DialAddr string

	initOnce sync.Once
	rpc      *rpc.Client
}

// Call interacts with the remote function `name` via RPC.
func (c *Client) Call(name string, args any, reply any) error {
	if c.rpc == nil {
		err := c.initRPC()
		if err != nil {
			return err
		}
	}

	return c.rpc.Call(name, args, reply)
}

// Internal function that initializes a new RPC client once.
func (c *Client) initRPC() error {
	var err error
	c.initOnce.Do(func() {
		client, innerErr := rpc.Dial("tcp", c.DialAddr)
		if innerErr != nil {
			err = innerErr
			return
		}
		c.rpc = client
	})
	return err
}

// Server represents an RPC plugin server where all plugins are registered.
type Server struct {
	closing chan chan error
}

// Register a new RPC service to the server.
func (s *Server) Register(name string, rsvc any) {
	rpc.RegisterName(name, rsvc)
}

// Serve the plugin server in the background.
// This method will automatically allocate an available port and start
// listening for incoming RPC calls. The function returns the allocated port.
//
// The serve loop uses the Go standard `net` library to accept tcp request
// and serve each RPC call in a separate goroutine.
// To avoid blocking server shutdown while accepting new requests on the TCP
// socket, I split listen and serve into two select cases. The two cases
// can mutually activate by sending booleans into the `accepting` or `serving`
// channels.
func (s *Server) Serve() (int, error) {

	l, err := net.Listen("tcp", ":")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port

	s.closing = make(chan chan error)
	serveLoop := func() {
		accepting := make(chan bool, 1)
		serving := make(chan net.Conn, 1)
		accepting <- true
		shutdown := false
		for {
			select {
			case errch := <-s.closing:
				shutdown = true
				err := l.Close()
				errch <- err
				return
			case <-accepting:
				go func() {
					conn, err := l.Accept()
					if err != nil {
						if !shutdown {
							fmt.Printf("error accepting connection: %v", err)
						}
						return
					}
					serving <- conn
				}()
			case conn := <-serving:
				go rpc.ServeConn(conn)
				accepting <- true
			}
		}
	}

	go serveLoop()

	return port, nil
}

// Shutdown gracefully terminates the RPC plugin server.
// This method will send a termination signal using the server's
// closing channel and wait for acknowledgement.
func (s *Server) Shutdown() error {
	rchan := make(chan error)
	s.closing <- rchan
	return <-rchan
}
