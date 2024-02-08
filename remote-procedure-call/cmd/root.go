package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const usage = `A command-line application that can call plugin extensions via RPC.

EXAMPLES:
  Call the Greeter plugin:
    <program> call Greeter

  Call the Curtime plugin to return the current year:
    <program> call Curtime 2006 `

var rootCmd = &cobra.Command{
	Use:   ".",
	Short: "A program to call plugin extensions via RPC",
	Long:  usage,
}

var callCmd = &cobra.Command{
	Use:   "call [plugin name] ...[plugin args]",
	Short: "call a plugin by its registered name",
	Long: `call is for calling a plugin extension via RPC.
Any custom plugin implemented in the "extensions" package can be called using "call"`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		pluginCall(callPluginCommand, args)
	},
}
var docsCmd = &cobra.Command{
	Use:   "doc [plugin name]",
	Short: "shows plugin documentation",
	Long:  `doc will retrieve plugin documentation and print it to stdout`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		pluginCall(docPluginCommand, args)
	},
}

func init() {
	rootCmd.AddCommand(callCmd, docsCmd)
}

// Execute the program using cobra
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
