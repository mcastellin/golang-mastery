package extensions

import "sync"

var modOnce sync.Once
var mods []Plugin

type Plugin interface {
	Name() string
	Do(*Input, *Reply) error
}

type Input struct {
	Args []string
}
type Reply struct {
	Message string
}

// Returns a list of all registered modules
func GetModules() []Plugin {
	modOnce.Do(func() {
		mods = []Plugin{
			// Add plugins here
			&greeter{},
			&curtime{},
			// End plugins
		}
	})
	return mods
}
