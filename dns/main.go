package main

import (
	"fmt"
	"net"
	"time"
)

const maxDatagramSize = 512

var store = map[string]string{
	"acme.com.":      "127.0.0.1",
	"blog.acme.com.": "127.0.0.1",
}

func forwardRequest(req []byte, upstream string) []byte {

	conn, err := net.DialTimeout("udp", upstream, 30*time.Second)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if _, err = conn.Write(req); err != nil {
		panic(err)
	}

	buf := make([]byte, maxDatagramSize)
	var n int
	if n, err = conn.Read(buf); err != nil {
		panic(err)
	}

	return buf[:n]

}

func main() {

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 53})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for {
		var buf [maxDatagramSize]byte
		n, addr, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			panic(err)
		}
		data := buf[:n]

		req := &DNS{}
		if err := req.Decode(data); err != nil {
			panic(err)
		}

		var response []byte
		for _, q := range req.Questions {
			if resolved, ok := store[string(q.Name)]; ok {
				rec := DNSResourceRecord{}
				rec.Name = q.Name
				rec.Type = DNSTypeA
				rec.Class = DNSClassIN
				rec.IP = net.ParseIP(resolved)

				req.Answers = append(req.Answers, rec)
				req.ANCount = 1
				req.QR = true
				response = req.Serialize()
			}
		}

		if response == nil {
			response = forwardRequest(data, "8.8.8.8:53")
			respRec := &DNS{}
			respRec.Decode(response)
			fmt.Println(respRec)
		}

		if _, err := conn.WriteToUDP(response, addr); err != nil {
			panic(err)
		}
	}
}
