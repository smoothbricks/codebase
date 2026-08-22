// Native sidecar entrypoint for @smoothbricks/lmao-ttsc.
//
// ttsc links the transform into a compiler host as a library (see ../driver);
// this binary exists so the same registration can be driven directly, without a
// JavaScript launcher. Importing the driver package runs its init(), so
// ttsc's utility host finds the plugin in the registry and every subcommand
// below transforms exactly as the linked build does.
package main

import (
	"fmt"
	"os"

	"github.com/samchon/ttsc/packages/ttsc/utility"

	lmao "smoothbricks.dev/lmao-ttsc-plugin/driver"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "%s: command required (expected build|transform|check|version)\n", lmao.PluginName)
		return 2
	}
	switch args[0] {
	case "-v", "--version", "version":
		fmt.Fprintf(os.Stdout, "%s %s\n", lmao.PluginName, lmao.PluginVersion)
		return 0
	case "build":
		return utility.RunBuild(args[1:])
	case "transform":
		return utility.RunTransform(args[1:])
	case "check":
		return utility.RunCheck(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "%s: unknown command %q\n", lmao.PluginName, args[0])
		return 2
	}
}
