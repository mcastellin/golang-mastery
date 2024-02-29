package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// structs intentionally left blank
type DNSSOA struct{}
type DNSSRV struct{}
type DNSMX struct{}
type DNSOPT struct{}
type DNSURI struct{}

type DNSOpCode uint8
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

type DNSQuestion struct {
	Name  []byte
	Type  DNSType
	Class DNSClass
}

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

func (q *DNSQuestion) Encode(bytes []byte, offset int) int {

	offset = encodeName(q.Name, bytes, offset)
	binary.BigEndian.PutUint16(bytes[offset:], uint16(q.Type))
	binary.BigEndian.PutUint16(bytes[offset+2:], uint16(q.Class))

	return offset + 4
}

func (q *DNSQuestion) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("Name: %s ", string(q.Name)))
	buf.WriteString(fmt.Sprintf("Type: %d ", q.Type))
	buf.WriteString(fmt.Sprintf("Class: %d ", q.Class))

	return buf.String()
}

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

func (r *DNSResourceRecord) decodeRData() error {
	fmt.Println(r.Type)
	switch r.Type {
	case DNSTypeA:
		r.IP = r.RData
	default:
		// This is a toy project and this case was not handled
		return errNotImplemented
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
		return 0
	}
}

func (r *DNSResourceRecord) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Name: %s ", r.Name))
	buf.WriteString(fmt.Sprintf("Type: %d ", r.Type))
	buf.WriteString(fmt.Sprintf("Class: %d ", r.Class))
	buf.WriteString(fmt.Sprintf("IP: %s ", r.IP))
	return buf.String()
}

type DNS struct {
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

	Questions   []DNSQuestion
	Answers     []DNSResourceRecord
	Authorities []DNSResourceRecord
	Additionals []DNSResourceRecord
}

func (d *DNS) Decode(data []byte) error {

	if len(data) < 12 {
		return errDNSPacketTooShort
	}
	// DNS header section
	//
	//  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	//  |                      ID                       |
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	//  |QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	//  |                    QDCOUNT                    |
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	//  |                    ANCOUNT                    |
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	//  |                    NSCOUNT                    |
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	//  |                    ARCOUNT                    |
	//  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

	d.ID = binary.BigEndian.Uint16(data[:2])
	d.Opcode = DNSOpCode(data[2]>>3) & 0x0F
	d.QR = data[2]&0x80 != 0
	d.AA = data[2]&0x04 != 0
	d.TC = data[2]&0x02 != 0
	d.RD = data[2]&0x01 != 0
	d.RA = data[3]&0x80 != 0
	d.Z = uint8(data[3]>>4) & 0x7

	d.ResponseCode = DNSResponseCode(data[3]) & 0x0F
	d.QDCount = binary.BigEndian.Uint16(data[4:6])
	d.ANCount = binary.BigEndian.Uint16(data[6:8])
	d.NSCount = binary.BigEndian.Uint16(data[8:10])
	d.ARCount = binary.BigEndian.Uint16(data[10:12])

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

	d.Additionals = d.Additionals[:0]
	// TODO having issues decoding additionals that I don't fully understand
	//for i := 0; i < int(d.ARCount); i++ {
	//var additional DNSResourceRecord
	//var err error
	//if offset, err = additional.Decode(data, offset); err != nil {
	//return err
	//}
	//d.Additionals = append(d.Additionals, additional)
	//}

	return nil
}

func (d *DNS) Serialize() []byte {
	dgSize := 12 // initialize with DNS header size

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
	for _, rr := range d.Additionals {
		dgSize += rr.computeSize()
	}

	bytes := make([]byte, dgSize)

	binary.BigEndian.PutUint16(bytes, d.ID)
	bytes[2] = b2i(d.QR)<<7 | uint8(d.Opcode<<3) | b2i(d.AA)<<2 | b2i(d.TC)<<1 | b2i(d.RD)
	bytes[3] = b2i(d.RA)<<7 | d.Z<<4 | byte(d.ResponseCode)

	binary.BigEndian.PutUint16(bytes[4:], d.QDCount)
	binary.BigEndian.PutUint16(bytes[6:], d.ANCount)
	//binary.BigEndian.PutUint16(bytes[8:], d.NSCount)
	//binary.BigEndian.PutUint16(bytes[10:], d.ARCount)
	binary.BigEndian.PutUint16(bytes[8:], 0x0000)
	binary.BigEndian.PutUint16(bytes[10:], 0x0000)

	offset := 12
	for _, q := range d.Questions {
		offset = q.Encode(bytes, offset)
	}

	for _, an := range d.Answers {
		offset = an.Encode(bytes, offset)
	}

	return bytes
}

func b2i(v bool) byte {
	if v {
		return 0x01
	}
	return 0x00
}

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

	buf.WriteString("\n;; ADDITIONALS SECTION")
	for _, a := range d.Additionals {
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

var (
	errNotImplemented       = errors.New("not implemented yet")
	errDNSPacketTooShort    = errors.New("dns packet too short")
	errReservedForFutureUse = errors.New("reserved for future use")
)
