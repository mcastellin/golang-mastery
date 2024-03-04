package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mcastellin/golang-mastery/dns-server/pkg/dns"
)

var upstreamResolverAddr = "8.8.8.8:53"

var dnsServePort = 53

var docstring = fmt.Sprintf(`DNS playground
WARN: THIS IS NOT A PRODUCTION GRADE APPLICATION!

To test DNS lookup use the following command and should resolve 127.0.0.0:
> dig @localhost blog.acme.com

serving UDP requests at port %d...`, dnsServePort)

func main() {
	store := dns.DNSLocalStore{}
	if err := store.FromFile("dns-records.txt"); err != nil {
		panic(err)
	}

	resolver := &dns.DNSResolver{
		Fwd:     &dns.DNSForwarder{Upstream: upstreamResolverAddr},
		Records: store,
	}

	srv := &DNSServer{Port: dnsServePort, Resolver: resolver}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Println(docstring)
	srv.Serve(ctx)
}
