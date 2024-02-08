package extensions

import "sync"

var modOnce sync.Once
var mods []Plugin

// The Plugin type represents the interface for every plugin definition.
type Plugin interface {
	Name() string
	Do(*Input, *Reply) error
	Docs(*Input, *Reply) error
}

// Input represents the RPC input structure for plugins
type Input struct {
	Args []string
}

// Reply represents the RPC reply structure for plugins
type Reply struct {
	Message string
}

// GetModules returns a list of all registered modules
func GetModules() []Plugin {
	modOnce.Do(func() {
		mods = []Plugin{
			// ADD PLUGINS HERE
			&greeter{},
			&curtime{},
			// END PLUGINS
		}
	})
	return mods
}
