package main

import (
	"fmt"
	"io"
	"runtime/debug"
)

var version = "dev"
var commit = "unknown"

func printVersion(args []string, w io.Writer) bool {
	if len(args) != 1 || (args[0] != "--version" && args[0] != "-version") {
		return false
	}
	v := version
	if v == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			v = info.Main.Version
		}
	}
	fmt.Fprintf(w, "hmq %s (commit %s)\n", v, commit)
	return true
}
