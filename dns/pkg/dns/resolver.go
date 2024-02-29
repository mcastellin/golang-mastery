package dns

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const defaultDialTimeout = 5 * time.Second
const defaultAnswerTTL = 300
const MaxDNSDatagramSize = 512

type DNSLocalStore map[string]string

func (store *DNSLocalStore) FromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	scan := bufio.NewScanner(file)
	for scan.Scan() {
		line := scan.Text()
		strings.Trim(line, " ")
		if strings.HasPrefix(line, ";") || len(line) == 0 {
			continue
		}
		k, v, err := parseLine(line)
		if err != nil {
			return err
		}
		(*store)[k] = v
	}

	return nil
}

func parseLine(line string) (string, string, error) {
	tokens := strings.SplitN(line, " ", 2)
	if len(tokens) != 2 {
		return "", "", fmt.Errorf("malformed DNS record. format should be 'example.com  10.0.1.55'")
	}

	k, v := strings.Trim(tokens[0], " "), strings.Trim(tokens[1], " ")
	return k, v, nil
}

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
			an.TTL = defaultAnswerTTL
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
