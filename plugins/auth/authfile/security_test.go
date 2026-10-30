package acl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseFileBoundaries(t *testing.T) {
	if _, err := AclConfigLoad(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing file should fail")
	}
	path := filepath.Join(t.TempDir(), "acl")
	if err := os.WriteFile(path, []byte("allow username first 1 a\n\nallow username second 1 b\n# end"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := AclConfigLoad(path)
	if err != nil || len(c.Info) != 2 {
		t.Fatalf("parse: %#v, %v", c, err)
	}
}
