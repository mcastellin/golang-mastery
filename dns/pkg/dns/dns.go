// Package dns implements **MINIMAL** support for DNS datagrams needed to read
// DNS questions and reply with A-type records.
//
// This is a playground module I use to learn how to process requests by implementing
// UDP protocols from RFC 1034 - RFC 1035 specifications.
package dns

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// Structs intentionally left blank
// This package DOES NOT fully implement DNS specifications as it's
// only meant to be used as part of this toy project and an opportunity
// to learn how to read and send UDP datagrams.
type DNSSOA struct{}
type DNSSRV struct{}
type DNSMX struct{}
type DNSOPT struct{}
type DNSURI struct{}

type DNSOpCode uint8

const (
	DNSOpCodeSTD    DNSOpCode = 0 // a standard query (QUERY)
	DNSOpCodeINV    DNSOpCode = 1 // an inverse query (IQUERY)
	DNSOpCodeSTATUS DNSOpCode = 2 // a server status request (STATUS)
	// 3-15 reserved for future use
)

type DNSResponseCode uint8

const (
	DNSResponseCodeNoError        DNSResponseCode = 0
	DNSResponseCodeFormatError    DNSResponseCode = 1
	DNSResponseCodeServerFailure  DNSResponseCode = 2
	DNSResponseCodeNameError      DNSResponseCode = 3
	DNSResponseCodeNotImplemented DNSResponseCode = 4
	DNSResponseCodeRefused        DNSResponseCode = 5
)

type DNSType uint16

const (
	DNSTypeA     DNSType = 1  // a host address
	DNSTypeNS    DNSType = 2  // an authoritative name server
	DNSTypeMD    DNSType = 3  // a mail destination (Obsolete, use MX)
	DNSTypeMF    DNSType = 4  // a mail forwarder (Obsolete, use MX)
	DNSTypeCNAME DNSType = 5  // the canonical name for an alias
	DNSTypeSOA   DNSType = 6  // marks the start of a zone of authority
	DNSTypeMB    DNSType = 7  // a mailbox domain name (EXPERIMENTAL)
	DNSTypeMG    DNSType = 8  // a mail group member (EXPERIMENTAL)
	DNSTypeMR    DNSType = 9  // a mail rename domain name (EXPERIMENTAL)
	DNSTypeNULL  DNSType = 10 // a null RR (EXPERIMENTAL)
	DNSTypeWKS   DNSType = 11 // a well known service description
	DNSTypePTR   DNSType = 12 // a domain name pointer
	DNSTypeHINFO DNSType = 13 // host information
	DNSTypeMINFO DNSType = 14 // mailbox or mail list information
	DNSTypeMX    DNSType = 15 // mail exchange
	DNSTypeTXT   DNSType = 16 // text strings
)

type DNSClass uint16

const (
	DNSClassIN DNSClass = 1 // the Internet
	DNSClassCS DNSClass = 2 // the CSNET class (Obsolete - used only for examples in some obsolete RFCs)
	DNSClassCH DNSClass = 3 // the CHAOS class
	DNSClassHS DNSClass = 4 // Hesiod [Dyer 87]
)

// DNSQuestion represents a DNS question structure in the datagram
//
// 0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                                               |
// /                     QNAME                     /
// /                                               /
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                     QTYPE                     |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                     QCLASS                    |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
type DNSQuestion struct {
	Name  []byte
	Type  DNSType
	Class DNSClass
}

// Decode the DNSQuestion struct from binary data.
func (q *DNSQuestion) Decode(data []byte, offset int) (int, error) {
	if len(data) < 6 {
		return 0, errDNSPacketTooShort
	}

	var err error
	if q.Name, offset, err = decodeName(data, offset); err != nil {
		return 0, err
	}

	q.Type = DNSType(binary.BigEndian.Uint16(data[offset : offset+2]))
	q.Class = DNSClass(binary.BigEndian.Uint16(data[offset+2 : offset+4]))

	return offset + 4, nil
}

// Encode binary data from a DNSQuestion struct
func (q *DNSQuestion) Encode(bytes []byte, offset int) int {

	offset = encodeName(q.Name, bytes, offset)
	binary.BigEndian.PutUint16(bytes[offset:], uint16(q.Type))
	binary.BigEndian.PutUint16(bytes[offset+2:], uint16(q.Class))

	return offset + 4
}

func (q *DNSQuestion) computeSize() int {
	// Name len + name termination + dnsType + dnsClass
	return len(q.Name) + 1 + 4
}

// String representation of the DNSQuestion struct
func (q *DNSQuestion) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("Name: %s ", string(q.Name)))
	buf.WriteString(fmt.Sprintf("Type: %d ", q.Type))
	buf.WriteString(fmt.Sprintf("Class: %d ", q.Class))

	return buf.String()
}

// DNSResourceRecord represents a RR in the datagram
//
// 0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                                               |
// /                                               /
// /                      NAME                     /
// |                                               |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                      TYPE                     |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                     CLASS                     |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                      TTL                      |
// |                                               |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                   RDLENGTH                    |
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--|
// /                     RDATA                     /
// /                                               /
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
type DNSResourceRecord struct {
	Name     []byte
	Type     DNSType
	Class    DNSClass
	TTL      uint32
	RDLenght uint16
	RData    []byte

	IP             net.IP
	NS, CNAME, PTR []byte
	TXTs           [][]byte
	SOA            DNSSOA
	SRV            DNSSRV
	MX             DNSMX
	OPT            []DNSOPT
	URI            DNSURI

	TXT []byte
}

// Decode a DNSResourceRecord struct from binary data
func (r *DNSResourceRecord) Decode(data []byte, offset int) (int, error) {

	var err error
	if r.Name, offset, err = decodeName(data, offset); err != nil {
		return 0, err
	}
	r.Type = DNSType(binary.BigEndian.Uint16(data[offset : offset+2]))
	r.Class = DNSClass(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
	r.TTL = binary.BigEndian.Uint32(data[offset+4 : offset+8])
	r.RDLenght = binary.BigEndian.Uint16(data[offset+8 : offset+10])

	rdEnd := offset + 10 + int(r.RDLenght)
	r.RData = data[offset+10 : rdEnd]
	if err := r.decodeRData(); err != nil {
		return 0, err
	}

	return offset + 10 + int(r.RDLenght), nil
}

// decodeRData into struct properties
func (r *DNSResourceRecord) decodeRData() error {
	fmt.Println(r.Type)
	switch r.Type {
	// For the purpose of this project we only decode RData for A records
	case DNSTypeA:
		r.IP = r.RData
	}
	return nil
}

func (r *DNSResourceRecord) computeSize() int {
	rSize := len(r.Name) + 1

	switch r.Type {
	case DNSTypeA:
		// IP addr
		rSize += 4
	}

	return rSize + 10
}

// Encode DNSResourceRecord struct into binary data for transport
func (r *DNSResourceRecord) Encode(bytes []byte, offset int) int {
	offset = encodeName(r.Name, bytes, offset)

	binary.BigEndian.PutUint16(bytes[offset:], uint16(r.Type))
	binary.BigEndian.PutUint16(bytes[offset+2:], uint16(r.Class))
	binary.BigEndian.PutUint32(bytes[offset+4:], r.TTL)

	switch r.Type {
	case DNSTypeA:
		copy(bytes[offset+10:], r.IP.To4())
		r.RDLenght = uint16(4)
		binary.BigEndian.PutUint16(bytes[offset+8:], r.RDLenght)
		return offset + 10 + 4
	default:
		// For the purpose of this project we only encode RData for A records
		r.RDLenght = uint16(0)
		binary.BigEndian.PutUint16(bytes[offset+8:], r.RDLenght)
		return offset + 10
	}
}

// String representation of the DNSResourceRecord
func (r *DNSResourceRecord) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Name: %s ", r.Name))
	buf.WriteString(fmt.Sprintf("Type: %d ", r.Type))
	buf.WriteString(fmt.Sprintf("Class: %d ", r.Class))
	buf.WriteString(fmt.Sprintf("IP: %s ", r.IP))
	return buf.String()
}

// DNSHeader represents the dns header information in the datagram
//
//	0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
//	|                      ID                       |
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
//	|QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
//	|                    QDCOUNT                    |
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
//	|                    ANCOUNT                    |
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
//	|                    NSCOUNT                    |
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
//	|                    ARCOUNT                    |
//	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
type DNSHeader struct {
	ID     uint16
	QR     bool
	Opcode DNSOpCode

	AA bool  // Authoritative answer
	TC bool  // Truncated
	RD bool  // Recursion desired
	RA bool  // Recursion available
	Z  uint8 // Reserved (for future use)

	ResponseCode DNSResponseCode
	QDCount      uint16 // Number of questions to expect
	ANCount      uint16 // Number of answers to expect
	NSCount      uint16 // Number of authorities to expect
	ARCount      uint16 // Number of additional records to expect
}

// Decode DNSHeader struct from bytes
func (head *DNSHeader) Decode(data []byte) {
	head.ID = binary.BigEndian.Uint16(data[:2])
	head.Opcode = DNSOpCode(data[2]>>3) & 0x0F
	head.QR = data[2]&0x80 != 0
	head.AA = data[2]&0x04 != 0
	head.TC = data[2]&0x02 != 0
	head.RD = data[2]&0x01 != 0
	head.RA = data[3]&0x80 != 0
	head.Z = uint8(data[3]>>4) & 0x7

	head.ResponseCode = DNSResponseCode(data[3]) & 0x0F
	head.QDCount = binary.BigEndian.Uint16(data[4:6])
	head.ANCount = binary.BigEndian.Uint16(data[6:8])
	head.NSCount = binary.BigEndian.Uint16(data[8:10])
	head.ARCount = binary.BigEndian.Uint16(data[10:12])
}

// Encode DNSHeader struct into binary representation for transport
func (head *DNSHeader) Encode(bytes []byte, offset int) int {

	binary.BigEndian.PutUint16(bytes, head.ID)
	bytes[2] = b2i(head.QR)<<7 | uint8(head.Opcode<<3) | b2i(head.AA)<<2 | b2i(head.TC)<<1 | b2i(head.RD)
	bytes[3] = b2i(head.RA)<<7 | head.Z<<4 | byte(head.ResponseCode)

	binary.BigEndian.PutUint16(bytes[4:], head.QDCount)
	binary.BigEndian.PutUint16(bytes[6:], head.ANCount)
	binary.BigEndian.PutUint16(bytes[8:], head.NSCount)
	binary.BigEndian.PutUint16(bytes[10:], head.ARCount)

	return 12
}

func (head *DNSHeader) computeSize() int {
	return 12
}

// DNS struct represents the whole DNS datagram as per RFC 1034 - RFC 1035 specifications.
type DNS struct {
	DNSHeader

	Questions   []DNSQuestion
	Answers     []DNSResourceRecord
	Authorities []DNSResourceRecord

	// For the purpose of this project we don't care about
	// decoding additionals, we will simply store them as bytes
	// and add them back when encoding the packet.
	Additionals []byte
}

// Decode DNS struct from bytes
func (d *DNS) Decode(data []byte) error {

	if len(data) < 12 {
		return errDNSPacketTooShort
	}

	d.DNSHeader.Decode(data)

	d.Questions = d.Questions[:0]
	offset := 12
	for i := 0; i < int(d.QDCount); i++ {
		var q DNSQuestion
		var err error
		if offset, err = q.Decode(data, offset); err != nil {
			return err
		}
		d.Questions = append(d.Questions, q)
	}

	d.Answers = d.Answers[:0]
	for i := 0; i < int(d.ANCount); i++ {
		var answer DNSResourceRecord
		var err error
		if offset, err = answer.Decode(data, offset); err != nil {
			return err
		}
		d.Answers = append(d.Answers, answer)
	}

	d.Authorities = d.Authorities[:0]
	for i := 0; i < int(d.NSCount); i++ {
		var auth DNSResourceRecord
		var err error
		if offset, err = auth.Decode(data, offset); err != nil {
			return err
		}
		d.Authorities = append(d.Authorities, auth)
	}

	d.Additionals = data[offset:]

	return nil
}

// Serialize a DNS struct into binary data for transport.
func (d *DNS) Serialize() []byte {
	dgSize := d.DNSHeader.computeSize()

	for _, q := range d.Questions {
		// question + final byte + type + class
		dgSize += len(q.Name) + 1 + 4
	}

	for _, rr := range d.Answers {
		dgSize += rr.computeSize()
	}
	for _, rr := range d.Authorities {
		dgSize += rr.computeSize()
	}
	dgSize += len(d.Additionals)

	bytes := make([]byte, dgSize)
	offset := d.DNSHeader.Encode(bytes, 0)

	for _, q := range d.Questions {
		offset = q.Encode(bytes, offset)
	}

	for _, an := range d.Answers {
		offset = an.Encode(bytes, offset)
	}
	for _, ns := range d.Authorities {
		offset = ns.Encode(bytes, offset)
	}

	copy(bytes[offset:], d.Additionals)

	return bytes
}

// ReplyTo DNS request with resource records.
// This function will create a new DNS message with the specified
// rr (Resource Records) in the answer section.
func (d *DNS) ReplyTo(rr []DNSResourceRecord) *DNS {

	reply := &DNS{}
	reply.ID = d.ID
	reply.Opcode = d.Opcode

	reply.QR = true // is answer
	reply.AA = d.AA
	reply.TC = d.TC
	reply.RD = d.RD
	reply.RA = d.RA
	reply.Z = d.Z

	reply.ResponseCode = d.ResponseCode
	reply.QDCount = d.QDCount
	reply.ANCount = uint16(len(rr))
	reply.NSCount = d.NSCount
	reply.ARCount = d.ARCount

	reply.Questions = d.Questions
	reply.Answers = append(reply.Answers, rr...)
	reply.Authorities = d.Authorities
	reply.Additionals = d.Additionals
	return reply
}

// String representation of the DNS struct
func (d *DNS) String() string {
	var buf bytes.Buffer

	buf.WriteString("\n;; HEADER SECTION")
	buf.WriteString("\n")
	buf.WriteString(fmt.Sprintf("ID: %d ", d.ID))
	buf.WriteString(fmt.Sprintf("OpCode: %d ", d.Opcode))
	buf.WriteString(fmt.Sprintf("QR: %t ", d.QR))
	buf.WriteString(fmt.Sprintf("AA: %t ", d.AA))
	buf.WriteString(fmt.Sprintf("TC: %t ", d.TC))
	buf.WriteString(fmt.Sprintf("RD: %t ", d.RD))
	buf.WriteString(fmt.Sprintf("RA: %t ", d.RA))
	buf.WriteString(fmt.Sprintf("Z: %d ", d.Z))
	buf.WriteString(fmt.Sprintf("ResponseCode: %d\n", d.ResponseCode))

	buf.WriteString("\n;; QUESTION SECTION")
	for _, q := range d.Questions {
		buf.WriteString(fmt.Sprintf("\n- %s", q.String()))
	}

	buf.WriteString("\n;; ANSWER SECTION")
	for _, a := range d.Answers {
		buf.WriteString(fmt.Sprintf("\n- %s", a.String()))
	}

	buf.WriteString("\n;; AUTHORITIES SECTION")
	for _, a := range d.Authorities {
		buf.WriteString(fmt.Sprintf("\n- %s", a.String()))
	}

	return buf.String()
}

func decodeName(data []byte, offset int) ([]byte, int, error) {
	var name []byte
	for {
		switch data[offset] & 0xc0 {
		default:
			// labels
			length := int(data[offset])
			offset++
			if length == 0 {
				return name, offset, nil
			}
			name = append(name, data[offset:offset+length]...)
			name = append(name, '.')

			offset += length
		case 0xc0:
			// label pointer
			ptr := binary.BigEndian.Uint16(data[offset:offset+2]) & 0x3fff
			label, _, err := decodeName(data, int(ptr))
			if err != nil {
				return nil, offset, err
			}
			name = append(name, label...)
			return name, offset + 2, nil
		case 0x80:
			return nil, offset, errReservedForFutureUse
		case 0x40:
			return nil, offset, errReservedForFutureUse
		}
	}
}

func encodeName(name []byte, bytes []byte, offset int) int {
	if len(name) == 0 {
		bytes[offset] = 0x00
		return offset + 1
	}

	length := 0
	for i := range name {
		if name[i] == '.' {
			bytes[offset+i-length] = byte(length)
			length = 0
		} else {
			bytes[offset+i+1] = name[i]
			length++
		}
	}

	bytes[offset+len(name)+1] = 0x00
	return offset + len(name) + 1
}

// convert boolean value to bit representation
func b2i(v bool) byte {
	if v {
		return 0x1
	}
	return 0x0
}

var (
	errNotImplemented       = errors.New("not implemented yet")
	errDNSPacketTooShort    = errors.New("dns packet too short")
	errReservedForFutureUse = errors.New("reserved for future use")
)
