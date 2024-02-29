package dns

import (
	"net"
	"time"
)

const defaultDialTimeout = 5 * time.Second
const MaxDNSDatagramSize = 512

type DNSLocalStore map[string]string

type DNSResolver struct {
	Fwd     Forwarder
	Records DNSLocalStore
}

func (rr *DNSResolver) Resolve(req []byte) ([]byte, error) {
	var err error
	dnsReq := &DNS{}

	if err = dnsReq.Decode(req); err != nil {
		return nil, err
	}

	var reply []byte
	for _, q := range dnsReq.Questions {
		if resolved, ok := rr.Records[string(q.Name)]; ok {
			an := DNSResourceRecord{}
			an.Name = q.Name
			an.Type = DNSTypeA
			an.Class = DNSClassIN
			an.IP = net.ParseIP(resolved)
			reply = dnsReq.ReplyTo(an).Serialize()
		}
	}
	if reply == nil && rr.Fwd != nil {
		reply, err = rr.Fwd.Forward(req)
		if err != nil {
			return nil, err
		}
	}

	return reply, nil
}

type Forwarder interface {
	Forward(req []byte) ([]byte, error)
}

type DNSForwarder struct {
	Upstream string
	Timeout  time.Duration
}

func (ff *DNSForwarder) Forward(req []byte) ([]byte, error) {
	timeout := defaultDialTimeout
	if ff.Timeout != 0 {
		timeout = ff.Timeout
	}

	var err error
	var conn net.Conn
	if conn, err = net.DialTimeout("udp", ff.Upstream, timeout); err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err = conn.Write(req); err != nil {
		return nil, err
	}

	buf := make([]byte, MaxDNSDatagramSize)
	var n int
	if n, err = conn.Read(buf); err != nil {
		return nil, err
	}

	return buf[:n], nil
}
