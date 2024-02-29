package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"
)

func client() {
	conn, err := net.DialTimeout("udp", "localhost:7777", 30*time.Second)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("test"))
	if err != nil {
		panic(err)
	}

	data, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		panic(err)
	}
	fmt.Print(data)
}

func server(ctx context.Context) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 7777})
	if err != nil {
		panic(err)
	}
	go serveLoop(conn, ctx)
}

func serveLoop(conn *net.UDPConn, ctx context.Context) {
	defer conn.Close()

	accepting := make(chan struct{}, 1)
	var buf [512]byte
	accepting <- struct{}{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-accepting:
			_, addr, err := conn.ReadFromUDP(buf[0:])
			if err != nil {
				panic(err)
			}

			reply := fmt.Sprintf("echo > %s\n", string(buf[0:]))
			go echoLoop(conn, addr, reply)

			accepting <- struct{}{}
		}
	}
}

func echoLoop(conn *net.UDPConn, addr *net.UDPAddr, echo string) {
	for {
		time.Sleep(time.Second)
		_, err := conn.WriteToUDP([]byte(echo), addr)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("generating")
	}
}

func testMain() {

	ctx, cancel := context.WithCancel(context.Background())

	go server(ctx)

	client()
	time.Sleep(5 * time.Second)

	cancel()
}
