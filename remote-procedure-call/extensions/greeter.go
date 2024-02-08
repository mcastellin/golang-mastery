package extensions

// A simple plugin that sends a greeting message.
// Useful for debugging the plugin system since there's not much that
// can go wrong with its code.
type greeter struct{}

func (p *greeter) Name() string {
	return "Greeter"
}

// Sends a greeting message
func (p *greeter) Do(_ *Input, reply *Reply) error {
	reply.Message = "Hello there!"

	return nil
}

// Renders docstring for greeter plugin
func (p *greeter) Docs(_ *Input, reply *Reply) error {
	reply.Message = `Greeter plugin

A plugin to send a greeting message back to the user.

Name: Greeter
Args: None`

	return nil
}
