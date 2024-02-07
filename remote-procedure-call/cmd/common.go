package cmd

import (
	"fmt"

	"github.com/mcastellin/golang-mastery/remote-procedure-call/extensions"
	"github.com/mcastellin/golang-mastery/remote-procedure-call/plugin"
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
