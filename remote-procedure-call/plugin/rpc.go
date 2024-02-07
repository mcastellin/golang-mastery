package plugin

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Client struct {
	DialAddr string
	initOnce sync.Once
	rpc      *rpc.Client
}

func (c *Client) Call(name string, args any, reply any) error {
	if c.rpc == nil {
		err := c.initRpc()
		if err != nil {
			return err
		}
	}

	return c.rpc.Call(name, args, reply)
}

func (c *Client) initRpc() error {
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

type Server struct {
	closing chan chan error
}

func (s *Server) Register(name string, rsvc any) {
	rpc.RegisterName(name, rsvc)
}

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

func (s *Server) Shutdown() error {
	rchan := make(chan error)
	s.closing <- rchan
	return <-rchan
}
