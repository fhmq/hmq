// Package storage provides a single-node durable append log. It does not
// implement replication or integration with the broker.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const MaxRecordSize = 16 << 20
const headerSize = 16

var ErrClosed = errors.New("storage: log closed")
var ErrCorrupt = errors.New("storage: corrupt log")
var ErrTooLarge = errors.New("storage: record too large")
var ErrLocked = errors.New("storage: log already locked")
var ErrUnsupported = errors.New("storage: platform unsupported")

// Log serializes appends, reads and close. A failed write or sync poisons the
// instance: reopen it to recover the on-disk state before further operations.
type Log struct {
	mu   sync.Mutex
	f    *os.File
	err  error
	path string
}

// Open opens a log file and rejects incomplete or corrupt records.
// On supported platforms Open holds an advisory exclusive file lock until
// Close. Callers must not replace, unlink, or write the file outside this API.
func Open(path string) (*Log, error) {
	if !platformSupported {
		return nil, ErrUnsupported
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockFile(f); err != nil {
		f.Close()
		return nil, err
	}
	// An opener may hold an old descriptor across a concurrent replacement.
	info, err := f.Stat()
	current, statErr := os.Stat(path)
	if err != nil || statErr != nil || !os.SameFile(info, current) {
		f.Close()
		return nil, ErrLocked
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		resolved, err = filepath.Abs(resolved)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	// Sync even an existing file and directory: a previous creation attempt
	// may have failed before persisting the directory entry.
	if err = f.Sync(); err == nil {
		err = syncDirectory(filepath.Dir(resolved))
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	l := &Log{f: f, path: resolved}
	if err = l.scan(nil); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

func (l *Log) scan(visit func([]byte) error) error {
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var offset int64
	for {
		var h [headerSize]byte
		n, err := io.ReadFull(l.f, h[:])
		if err == io.EOF && n == 0 {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return fmt.Errorf("%w: incomplete header at %d", ErrCorrupt, offset)
		}
		if err != nil {
			return err
		}
		if string(h[:4]) != "HMQ1" || binary.BigEndian.Uint32(h[4:8]) != 1 {
			return fmt.Errorf("%w: header at %d", ErrCorrupt, offset)
		}
		size := binary.BigEndian.Uint32(h[8:12])
		if size > MaxRecordSize {
			return fmt.Errorf("%w: length at %d", ErrCorrupt, offset)
		}
		data := make([]byte, int(size))
		_, err = io.ReadFull(l.f, data)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return fmt.Errorf("%w: incomplete payload at %d", ErrCorrupt, offset)
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(h[12:16]) {
			return fmt.Errorf("%w: checksum at %d", ErrCorrupt, offset)
		}
		if visit != nil {
			if err := visit(data); err != nil {
				return err
			}
		}
		offset += int64(headerSize) + int64(size)
	}
	return nil
}

// Append writes a record and syncs it before returning success.
func (l *Log) Append(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	if l.err != nil {
		return l.err
	}
	if len(data) > MaxRecordSize {
		return ErrTooLarge
	}
	buf := make([]byte, headerSize+len(data))
	copy(buf, "HMQ1")
	binary.BigEndian.PutUint32(buf[4:8], 1)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(data)))
	binary.BigEndian.PutUint32(buf[12:16], crc32.ChecksumIEEE(data))
	copy(buf[headerSize:], data)
	if _, err := l.f.Seek(0, io.SeekEnd); err != nil {
		l.err = err
		return err
	}
	n, err := l.f.Write(buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = l.f.Sync()
	}
	if err != nil {
		l.err = err
	}
	return err
}

// ReadAll returns independent copies of all records in append order.
func (l *Log) ReadAll() ([][]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil, ErrClosed
	}
	if l.err != nil {
		return nil, l.err
	}
	var records [][]byte
	err := l.scan(func(data []byte) error {
		records = append(records, data)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// Replay visits validated records in append order, using memory bounded by one
// record unless visit retains records. Each payload is independently owned by
// the callback. The first callback or read error stops replay and is returned;
// callback errors do not poison the log. Earlier callbacks are not rolled back.
// Replay serializes with Append, ReadAll, Replay and Close; visit must not call
// any of these methods on the same Log. visit must not be nil.
func (l *Log) Replay(visit func([]byte) error) error {
	if visit == nil {
		return errors.New("storage: nil replay callback")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	if l.err != nil {
		return l.err
	}
	return l.scan(visit)
}

// Close closes the file; subsequent calls return ErrClosed.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	err := l.f.Close()
	l.f = nil
	return err
}
