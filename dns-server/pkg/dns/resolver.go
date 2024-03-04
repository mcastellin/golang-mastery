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

// MaxDNSDatagramSize represent the maximum size of DNS packets this
// application will accept.
const MaxDNSDatagramSize = 512

// DNSLocalStore is a minimal key-value datastore implementation
// to store local DNS record information.
//
// The keys in this datastore are the FQDNs and values are the
// associated IP addresses. It is also possible to use `BLOCK` as
// the resolved value for a fully qualified domain name to return
// an empty response for queries on certain domains.
type DNSLocalStore map[string]string

// FromFile loads the datastore initial state from a file.
//
// The datastore file contains one key-value pair per line that represent
// DNS A records:
//
// ; my records
// example.com.        10.0.0.3
// test.example.com.   10.0.0.2
// ; end my records
//
// Lines that start with a `;` character are interpreted as comments and
// blank lines are ignored.
func (store *DNSLocalStore) FromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

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

// DNSResolver replies to DNS queries by either finding matching A records
// in the local storage or forwarding requests to upstream servers.
type DNSResolver struct {
	Fwd     Forwarder
	Records DNSLocalStore
}

// Resolve DNS answers for the incoming request.
func (rr *DNSResolver) Resolve(req []byte) ([]byte, error) {
	var err error
	dnsReq := &DNS{}

	if err = dnsReq.Decode(req); err != nil {
		return nil, err
	}

	for _, q := range dnsReq.Questions {
		if resolved, ok := rr.Records[string(q.Name)]; ok {
			var answers []DNSResourceRecord
			switch resolved {
			default:
				an := DNSResourceRecord{}
				an.Name = q.Name
				an.Type = DNSTypeA
				an.Class = DNSClassIN
				an.IP = net.ParseIP(resolved)
				an.TTL = defaultAnswerTTL
				answers = []DNSResourceRecord{an}
			case "BLOCK":
				answers = []DNSResourceRecord{}
			}

			reply := dnsReq.ReplyTo(answers).Serialize()
			return reply, nil
		}
	}

	// If DNS recursion desired (RD) flag is set and forward server is available,
	// proxy the DNS request.
	if dnsReq.RD && rr.Fwd != nil {
		reply, err := rr.Fwd.Forward(req)
		if err != nil {
			return nil, err
		}
		return reply, nil
	}

	empty := dnsReq.ReplyTo([]DNSResourceRecord{}).Serialize()
	return empty, nil
}

// Forwarder is the interface implemented by DNS request forwarders.
type Forwarder interface {
	Forward(req []byte) ([]byte, error)
}

// DNSForwarder implements logic to forward raw DNS requests to upstream
// DNS servers when recursion is requested.
type DNSForwarder struct {
	Upstream    string
	DialTimeout time.Duration
}

// Forward the raw DNS request to upstream server.
func (ff *DNSForwarder) Forward(req []byte) ([]byte, error) {
	timeout := defaultDialTimeout
	if ff.DialTimeout != 0 {
		timeout = ff.DialTimeout
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
