package main

import (
	"context"
	"fmt"
	"net"

	"github.com/mcastellin/golang-mastery/dns/pkg/dns"
)

type Resolver interface {
	Resolve([]byte) ([]byte, error)
}

type DNSServer struct {
	Port     int
	Resolver Resolver
	shutdown bool
}

func (srv *DNSServer) Serve(ctx context.Context) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: srv.Port})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	srv.serveLoop(ctx, conn)
}

func (srv *DNSServer) serveLoop(ctx context.Context, conn *net.UDPConn) {
	srv.shutdown = false
	accepting := make(chan struct{}, 1)
	type wrapper struct {
		data []byte
		addr *net.UDPAddr
	}
	serving := make(chan wrapper, 1)

	accept := func() {
		if !srv.shutdown {
			accepting <- struct{}{}
		}
	}
	accept()

	acceptFn := func() {
		defer accept() // accept a new request when done
		var buf [dns.MaxDNSDatagramSize]byte
		n, addr, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			if recoverable := srv.handleErr(err); !recoverable {
				return
			}
		}
		data := buf[:n]
		serving <- wrapper{data, addr}
	}

	serveFn := func(data []byte, addr *net.UDPAddr) {
		var reply []byte
		var err error
		if reply, err = srv.Resolver.Resolve(data); err != nil {
			if recoverable := srv.handleErr(err); !recoverable {
				return
			}
		}

		if _, err := conn.WriteToUDP(reply, addr); err != nil {
			if recoverable := srv.handleErr(err); !recoverable {
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			// handle parent context cancellation and prepare
			// server for shutdown. As we are accepting requests
			// asynchronously we can expect errors in acceptFn
			// when the UDP connection is closed, hence using the
			// shutdown = true to signal the handler an error is
			// expected.
			srv.shutdown = true
			return

		case <-accepting:
			// spawn a subroutine to accept a new UDP connection request.
			// Accepting a new request is a blocking operation. To always
			// be responsive to application termination signals we delegate
			// this task to a separate goroutine.
			//
			// To ensure we only have one routine accepting requests at any
			// time, this case is activated only when a new struct{} is
			// is placed into the accepting channel. The first message is
			// added during the serve loop init, further activations are
			// handled by the routine itself with a defer.
			go acceptFn()

		case w := <-serving:
			// serve incoming DNS requests concurrently in subroutines.
			// Every incoming request is handled concurrently in a subroutine
			// to maximise throughput.
			//
			// TODO
			// At the moment we are not setting a maximum amount of concurrent
			// serve routines. Though we should refactor to throttle incoming
			// requests using a buffer.
			go serveFn(w.data, w.addr)
		}
	}
}

func (srv *DNSServer) handleErr(err error) bool {
	if !srv.shutdown {
		fmt.Println(err)
	}
	return false
}
