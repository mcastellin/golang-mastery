package cmd

import (
	"fmt"
	"os"

	"github.com/mcastellin/golang-mastery/remote-procedure-call/extensions"
	"github.com/mcastellin/golang-mastery/remote-procedure-call/plugin"
)

type pluginCommandType string

const (
	callPluginCommand pluginCommandType = "Do"
	docPluginCommand  pluginCommandType = "Docs"
)

func startPlugins() (*plugin.Server, *plugin.Client, error) {
	plugServer := &plugin.Server{}
	for _, mod := range extensions.GetModules() {
		plugServer.Register(mod.Name(), mod)
	}
	port, err := plugServer.Serve()
	if err != nil {
		return nil, nil, err
	}
	plugins := &plugin.Client{DialAddr: fmt.Sprintf(":%d", port)}
	return plugServer, plugins, nil
}

func pluginCall(command pluginCommandType, args []string) {
	server, client, err := startPlugins()
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	defer server.Shutdown()

	plugName := args[0]

	inArgs := &extensions.Input{Args: args[1:]}
	reply := &extensions.Reply{}
	err = client.Call(fmt.Sprintf("%s.%s", plugName, command), inArgs, reply)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(reply.Message)
}
