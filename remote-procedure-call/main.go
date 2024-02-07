package main

import (
	"fmt"
	"os"

	"github.com/mcastellin/golang-mastery/remote-procedure-call/cmd"
)

const usage = `
A command-line application that can call plugin extensions via RPC.

USAGE: <program> call [PluginName] ...[Args]

COMMANDS:
call:
  performs an RPC call a registered plugin by name.

doc:
  prints the plugin docstring.

EXAMPLES:
  Call the Greeter plugin:
    <program> call Greeter

  Call the Curtime plugin to return the current year:
    <program> call Curtime 2006 `

func main() {
	if len(os.Args) <= 1 {
		fmt.Println(usage)
		return
	}
	cmd.CallCmd()

}
