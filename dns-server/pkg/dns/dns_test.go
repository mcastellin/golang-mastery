package dns

import (
	"slices"
	"testing"
)

// DNS query dump from dig for amazon.com
//
// 0000   7b 65 01 20 00 01 00 00 00 00 00 01 06 61 6d 61   {e. .........ama
// 0010   7a 6f 6e 03 63 6f 6d 00 00 01 00 01 00 00 29 10   zon.com.......).
// 0020   00 00 00 00 00 00 00                              .......
var testQuery = []byte{
	0x7b, 0x65, 0x01, 0x20, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x61, 0x6d, 0x61,
	0x7a, 0x6f, 0x6e, 0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x29, 0x10,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

// DNS query response dump for amazon.com
//
// 0000   7b 65 81 80 00 01 00 03 00 00 00 01 06 61 6d 61   {e...........ama
// 0010   7a 6f 6e 03 63 6f 6d 00 00 01 00 01 c0 0c 00 01   zon.com.........
// 0020   00 01 00 00 02 fa 00 04 36 ef 1c 55 c0 0c 00 01   ........6..U....
// 0030   00 01 00 00 02 fa 00 04 cd fb f2 67 c0 0c 00 01   ...........g....
// 0040   00 01 00 00 02 fa 00 04 34 5e ec f8 00 00 29 02   ........4^....).
// 0050   00 00 00 00 00 00 00                              .......
var testQueryResponse = []byte{
	0x7b, 0x65, 0x81, 0x80, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x06, 0x61, 0x6d, 0x61,
	0x7a, 0x6f, 0x6e, 0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01, 0xc0, 0x0c, 0x00, 0x01,
	0x00, 0x01, 0x00, 0x00, 0x02, 0xfa, 0x00, 0x04, 0x36, 0xef, 0x1c, 0x55, 0xc0, 0x0c, 0x00, 0x01,
	0x00, 0x01, 0x00, 0x00, 0x02, 0xfa, 0x00, 0x04, 0xcd, 0xfb, 0xf2, 0x67, 0xc0, 0x0c, 0x00, 0x01,
	0x00, 0x01, 0x00, 0x00, 0x02, 0xfa, 0x00, 0x04, 0x34, 0x5e, 0xec, 0xf8, 0x00, 0x00, 0x29, 0x02,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

// Bytes representation of DNS response
var testEncodingRegression = []byte{
	0x7b, 0x65, 0x81, 0x80, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x06, 0x61, 0x6d, 0x61,
	0x7a, 0x6f, 0x6e, 0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01, 0x06, 0x61, 0x6d, 0x61,
	0x7a, 0x6f, 0x6e, 0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2c,
	0x00, 0x04, 0x36, 0xef, 0x1c, 0x55, 0x06, 0x61, 0x6d, 0x61, 0x7a, 0x6f, 0x6e, 0x03, 0x63, 0x6f,
	0x6d, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2c, 0x00, 0x04, 0xcd, 0xfb, 0xf2, 0x67,
	0x06, 0x61, 0x6d, 0x61, 0x7a, 0x6f, 0x6e, 0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01,
	0x00, 0x00, 0x01, 0x2c, 0x00, 0x04, 0x34, 0x5e, 0xec, 0xf8, 0x00, 0x00, 0x29, 0x10, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00,
}

func TestDecode(t *testing.T) {
	result := &DNS{}
	result.Decode(testQuery)

	if len(result.Questions) != 1 {
		t.Fatalf("wrong number of questions decoded: expected 1, got %d", len(result.Questions))
	}

	if int(result.QDCount) != 1 {
		t.Fatalf("wrong number of questions decoded: expected 1, got %d", result.QDCount)
	}

	q := result.Questions[0]
	if string(q.Name) != "amazon.com." {
		t.Fatalf("wrong question name: expected amazon.com., found: %s", string(q.Name))
	}
}

func TestEncodeRegression(t *testing.T) {

	req := &DNS{}
	req.Decode(testQuery)

	answers := make([]DNSResourceRecord, 3)
	answers[0] = DNSResourceRecord{
		Name:  req.Questions[0].Name,
		Type:  DNSTypeA,
		Class: DNSClassIN,
		TTL:   300,
		IP:    []byte{54, 239, 28, 85},
	}
	answers[1] = DNSResourceRecord{
		Name:  req.Questions[0].Name,
		Type:  DNSTypeA,
		Class: DNSClassIN,
		TTL:   300,
		IP:    []byte{205, 251, 242, 103},
	}
	answers[2] = DNSResourceRecord{
		Name:  req.Questions[0].Name,
		Type:  DNSTypeA,
		Class: DNSClassIN,
		TTL:   300,
		IP:    []byte{52, 94, 236, 248},
	}

	bytes := req.ReplyTo(answers).Serialize()

	if !slices.Equal(bytes, testEncodingRegression) {
		t.Fatal("DNS packet encoding regression found.")
	}
}
