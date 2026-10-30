package main

import (
	"bytes"
	"testing"
)

func TestPrintVersion(t *testing.T) {
	oldVersion, oldCommit := version, commit
	version, commit = "v3.0.0-test", "test-commit"
	t.Cleanup(func() { version, commit = oldVersion, oldCommit })
	for _, args := range [][]string{{"--version"}, {"-version"}} {
		var out bytes.Buffer
		if !printVersion(args, &out) || out.String() != "hmq "+version+" (commit "+commit+")\n" {
			t.Fatalf("version output: %q", out.String())
		}
	}
	for _, args := range [][]string{nil, {"--help"}, {"--version", "extra"}} {
		var out bytes.Buffer
		if printVersion(args, &out) || out.Len() != 0 {
			t.Fatalf("unexpected version handling: %v", args)
		}
	}
}
