package extensions

import "time"

const DefaultLayout string = time.RFC822

// A simple plugin that renders a string representation of the current time.
// This plugin will accept a time layout as the first argument in the call. If
// none is provided a default representation will be used.
type curtime struct{}

func (p *curtime) Name() string {
	return "Curtime"
}

// Sends a greeting message
func (p *curtime) Do(args *Input, reply *Reply) error {
	layout := DefaultLayout
	if len(args.Args) > 0 && len(args.Args[0]) > 0 {
		layout = args.Args[0]
	}
	reply.Message = time.Now().Format(layout)

	return nil
}
