package cmd

import (
	"fmt"
	"os"

	"github.com/mcastellin/golang-mastery/remote-procedure-call/extensions"
)

func CallCmd() {
	server, client, err := startPlugins()
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	defer server.Shutdown()

	plugName := os.Args[1]

	args := &extensions.Input{Args: os.Args[2:]}
	reply := &extensions.Reply{}
	err = client.Call(fmt.Sprintf("%s.Do", plugName), args, reply)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(reply.Message)
}
