package extensions

import "time"

const defaultLayout string = time.RFC822

// A simple plugin that renders a string representation of the current time.
// This plugin will accept a time layout as the first argument in the call. If
// none is provided a default representation will be used.
type curtime struct{}

func (p *curtime) Name() string {
	return "Curtime"
}

// Sends a greeting message
func (p *curtime) Do(args *Input, reply *Reply) error {
	layout := defaultLayout
	if len(args.Args) > 0 && len(args.Args[0]) > 0 {
		layout = args.Args[0]
	}
	reply.Message = time.Now().Format(layout)

	return nil
}

// Renders docstring for curtime plugin
func (p *curtime) Docs(_ *Input, reply *Reply) error {
	reply.Message = `Curtime plugin

A plugin to render the current timestamp and send it back to the user.

Name: Curtime
Args: Curtime [format]
  - format: the date format expressed as a Golang time layout string. Example: Curtime 2006-1-2`

	return nil
}
