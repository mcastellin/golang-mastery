package main

import (
	"net"

	"github.com/mcastellin/golang-mastery/dns/pkg/dns"
)

var store = dns.DNSLocalStore{
	"acme.com.":      "127.0.0.1",
	"blog.acme.com.": "127.0.0.1",
}

var upstreamResolverAddr = "8.8.8.8:53"

func main() {

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 53})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	resolver := &dns.DNSResolver{
		Fwd:     &dns.DNSForwarder{Upstream: upstreamResolverAddr},
		Records: store,
	}

	for {
		var buf [dns.MaxDNSDatagramSize]byte
		n, addr, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			panic(err)
		}
		data := buf[:n]

		var reply []byte
		if reply, err = resolver.Resolve(data); err != nil {
			panic(err)
		}

		if _, err := conn.WriteToUDP(reply, addr); err != nil {
			panic(err)
		}
	}
}
