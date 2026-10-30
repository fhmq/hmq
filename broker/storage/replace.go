package storage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Replace atomically replaces the log with a caller-provided snapshot. The
// caller must preserve all recovery metadata and must not mutate records
// during this call. Success includes syncing the replacement and its directory.
// An error after rename poisons the log: close and reopen before further use.
func (l *Log) Replace(records [][]byte) error {
	return l.replace(records, syncDirectory)
}

func (l *Log) replace(records [][]byte, syncParent func(string) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	if l.err != nil {
		return l.err
	}
	unique, err := singleLink(l.f)
	if err != nil {
		return err
	}
	if !unique {
		return ErrLocked
	}
	for _, data := range records {
		if len(data) > MaxRecordSize {
			return ErrTooLarge
		}
	}
	f, err := os.CreateTemp(filepath.Dir(l.path), ".hmq-replace-*")
	if err != nil {
		return err
	}
	name := f.Name()
	installed := false
	defer func() {
		if !installed {
			_ = f.Close()
			_ = os.Remove(name)
		}
	}()
	if err = lockFile(f); err != nil {
		return err
	}
	for _, data := range records {
		var h [headerSize]byte
		copy(h[:], "HMQ1")
		binary.BigEndian.PutUint32(h[4:8], 1)
		binary.BigEndian.PutUint32(h[8:12], uint32(len(data)))
		binary.BigEndian.PutUint32(h[12:16], crc32.ChecksumIEEE(data))
		for _, part := range [][]byte{h[:], data} {
			n, writeErr := f.Write(part)
			if writeErr != nil {
				return writeErr
			}
			if n != len(part) {
				return io.ErrShortWrite
			}
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = os.Rename(name, l.path); err != nil {
		return err
	}
	installed = true
	old := l.f
	l.f = f
	err = syncParent(filepath.Dir(l.path))
	if closeErr := old.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		l.err = err
	}
	return err
}
