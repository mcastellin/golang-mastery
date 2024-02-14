package main

import (
	"fmt"
	"time"

	gossip "github.com/mcastellin/golang-mastery/gossip/pkg"
)

const (
	seedAddrPattern = "localhost:98%02d"
	nodeAddrPattern = "localhost:99%02d"
)

func main() {
	seedNodes := 2
	regularNodes := 4

	seeds := []string{}
	for i := 0; i < seedNodes; i++ {
		addr := fmt.Sprintf(seedAddrPattern, i)
		seeds = append(seeds, addr)
	}

	// Start seed nodes
	fmt.Println("Starting seed nodes:", seeds)
	for _, seed := range seeds {
		si := gossip.NewGossiper(seed, true, seeds)
		if err := si.Serve(); err != nil {
			panic(err)
		}
		defer si.Shutdown()
	}

	// Start regular cluster nodes
	fmt.Printf("Starting %d regular cluster nodes.\n", regularNodes)
	nodes := make([]*gossip.Gossiper, regularNodes)
	for i := 0; i < regularNodes; i++ {
		addr := fmt.Sprintf(nodeAddrPattern, i)
		si := gossip.NewGossiper(addr, false, seeds)
		if err := si.Serve(); err != nil {
			panic(err)
		}
		defer si.Shutdown()
		nodes[i] = si
	}

	go func() {
		// membership monitor
		for {
			selected := len(nodes) - 1
			time.Sleep(time.Second)
			online := nodes[selected].Nodes()
			fmt.Printf("Total nodes count for %s => %d\n", nodes[selected].BindAddr, len(online))
		}
	}()

	go func() {
		// temporary node loss
		time.Sleep(5 * time.Second)
		fmt.Println("*************** node down")
		nodes[0].Shutdown()
		nodes[1].Shutdown()

		time.Sleep(15 * time.Second)
		fmt.Println("*************** node up")
		nodes[0].Serve()
		nodes[1].Serve()
	}()

	time.Sleep(30 * time.Second)
}
