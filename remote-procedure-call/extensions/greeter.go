package extensions

// A simple plugin that sends a greeting message.
// Useful for debugging the plugin system since there's not much that
// can go wrong with its code.
type greeter struct{}

func (p *greeter) Name() string {
	return "Greeter"
}

// Sends a greeting message
func (p *greeter) Do(args *Input, reply *Reply) error {
	reply.Message = "Hello there!"

	return nil
}
