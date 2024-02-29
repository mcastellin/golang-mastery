package main

import (
	"fmt"
	"net"

	"github.com/mcastellin/golang-mastery/dns/pkg/dns"
)

var upstreamResolverAddr = "8.8.8.8:53"

const docstring = `DNS playground
WARN: THIS IS NOT A PRODUCTION GRADE APPLICATION!

To test DNS lookup use the following command and should resolve 127.0.0.0:
> dig @localhost blog.acme.com

serving UDP requests at port 53...`

func main() {

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 53})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	store := dns.DNSLocalStore{}
	if err := store.FromFile("dns-records.txt"); err != nil {
		panic(err)
	}

	resolver := &dns.DNSResolver{
		Fwd:     &dns.DNSForwarder{Upstream: upstreamResolverAddr},
		Records: store,
	}

	fmt.Println(docstring)

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
