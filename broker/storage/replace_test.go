package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestReplaceSerializesReaders(t *testing.T) {
	if !platformSupported {
		t.Skip("unsupported platform")
	}
	l, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Replace([][]byte{[]byte("a"), []byte("a")}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			records, err := l.ReadAll()
			if err != nil || len(records) != 2 || string(records[0]) != string(records[1]) {
				t.Errorf("partial snapshot: %q %v", records, err)
				return
			}
		}
	}()
	for i := 0; i < 10; i++ {
		data := []byte{byte('a' + i)}
		if err := l.Replace([][]byte{data, data}); err != nil {
			t.Error(err)
			break
		}
	}
	wg.Wait()
}

func TestReplaceRecoveryAndOwnership(t *testing.T) {
	if !platformSupported {
		t.Skip("unsupported platform")
	}
	path := filepath.Join(t.TempDir(), "log")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Append([]byte("obsolete")); err != nil {
		t.Fatal(err)
	}
	want := [][]byte{[]byte("identity"), []byte("pending")}
	for i := 0; i < 3; i++ {
		if err := l.Replace(want); err != nil {
			t.Fatal(err)
		}
		other, err := Open(path)
		if other != nil {
			other.Close()
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("replacement not locked: %v", err)
		}
	}
	if err := l.Append([]byte("later")); err != nil {
		t.Fatal(err)
	}
	want = append(want, []byte("later"))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, err := l.ReadAll()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("recovery: %q %v", got, err)
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".hmq-replace-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files: %v %v", files, err)
	}
}

func TestReplaceThroughSymlinkAndEmpty(t *testing.T) {
	if !platformSupported {
		t.Skip("unsupported platform")
	}
	dir := t.TempDir()
	path, alias := filepath.Join(dir, "log"), filepath.Join(dir, "alias")
	linkDir := filepath.Join(dir, "links")
	if err := os.Mkdir(linkDir, 0700); err != nil {
		t.Fatal(err)
	}
	alias = filepath.Join(linkDir, "alias")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	l, err := Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Append([]byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := l.Replace(nil); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(alias)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("alias replaced: %v", err)
	}
	got, err := l.ReadAll()
	if err != nil || len(got) != 0 {
		t.Fatalf("empty snapshot: %q %v", got, err)
	}
}

func TestReplaceDirectoryFailurePoisonsNewLog(t *testing.T) {
	if !platformSupported {
		t.Skip("unsupported platform")
	}
	path := filepath.Join(t.TempDir(), "log")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	failure := errors.New("directory sync failed")
	if err := l.replace([][]byte{[]byte("new")}, func(string) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := l.Append([]byte("later")); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := l.ReadAll(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	called := false
	if err := l.Replay(func([]byte) error { called = true; return nil }); !errors.Is(err, failure) || called {
		t.Fatalf("poisoned replay invoked callback: %v %v", called, err)
	}
	if err := l.Replace(nil); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("new log unlocked: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, err := l.ReadAll()
	if err != nil || len(got) != 1 || string(got[0]) != "new" {
		t.Fatalf("reopen: %q %v", got, err)
	}
}

func TestReplaceRejectsHardlinks(t *testing.T) {
	if !platformSupported {
		t.Skip("unsupported platform")
	}
	path := filepath.Join(t.TempDir(), "log")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := os.Link(path, path+"-alias"); err != nil {
		t.Fatal(err)
	}
	if err := l.Replace(nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("hardlinked replacement: %v", err)
	}
	if err := l.Append([]byte("still usable")); err != nil {
		t.Fatal(err)
	}
}
