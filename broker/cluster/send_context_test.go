package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Observe socket entry independently of whether net.Pipe has accepted any bytes.
type sendContextConn struct {
	net.Conn
	entered   chan struct{}
	once      sync.Once
	closed    atomic.Bool
	deadlines chan time.Time
}

func (c *sendContextConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

func (c *sendContextConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func (c *sendContextConn) SetWriteDeadline(d time.Time) error {
	select {
	case c.deadlines <- d:
	default:
	}
	return c.Conn.SetWriteDeadline(d)
}

func newSendContextPipe(t *testing.T, timeout time.Duration) (*Node, *peer, *sendContextConn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	c := &sendContextConn{Conn: a, entered: make(chan struct{}), deadlines: make(chan time.Time, 32)}
	p := &peer{conn: c, done: make(chan struct{})}
	n := &Node{
		cfg:   Config{MaxFrameSize: 1024, WriteTimeout: timeout},
		peers: map[string]*peer{"peer": p}, done: make(chan struct{}),
		broadcastSlots: make(chan struct{}, maxConcurrentBroadcasts),
	}
	t.Cleanup(func() { c.Close(); b.Close() })
	return n, p, c, b
}

func sendContextResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("context-aware send did not return promptly")
		return nil
	}
}

func sendContextEntered(t *testing.T, c *sendContextConn) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not enter socket Write")
	}
}

func TestSendContextCanceledBeforeWrite(t *testing.T) {
	n, _, c, _ := newSendContextPipe(t, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := n.SendContext(ctx, "peer", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled send: %v", err)
	}
	select {
	case <-c.entered:
		t.Fatal("already canceled context reached socket Write")
	default:
	}
	if c.closed.Load() {
		t.Fatal("cancel before writing closed an untouched connection")
	}
	if len(n.broadcastSlots) != 0 {
		t.Fatal("canceled send leaked a broadcast slot")
	}
}

func TestSendContextQueuedCancellationDoesNotInterruptWriter(t *testing.T) {
	n, _, c, receiver := newSendContextPipe(t, 5*time.Second)
	first := make(chan error, 1)
	go func() { first <- n.SendContext(context.Background(), "peer", []byte("first")) }()
	sendContextEntered(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- n.SendContext(ctx, "peer", []byte("second")) }()
	limit := time.Now().Add(time.Second)
	for len(n.broadcastSlots) != 2 && time.Now().Before(limit) {
		time.Sleep(time.Millisecond)
	}
	if len(n.broadcastSlots) != 2 {
		t.Fatal("second sender did not reach the occupied write lock")
	}
	cancel()
	if err := sendContextResult(t, second); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation: %v", err)
	}
	if c.closed.Load() {
		t.Fatal("queued cancellation closed another writer's connection")
	}
	select {
	case err := <-first:
		t.Fatalf("queued cancellation interrupted active writer: %v", err)
	default:
	}
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 5, 'f', 'i', 'r', 's', 't'}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(receiver, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("active writer frame = %q, %v", got, err)
	}
	if err := sendContextResult(t, first); err != nil {
		t.Fatalf("active writer: %v", err)
	}
	// Reuse immediately: a canceled waiter's deadline/callback must not poison it.
	third := make(chan error, 1)
	go func() { third <- n.SendContext(context.Background(), "peer", []byte("third")) }()
	if _, err := io.ReadFull(receiver, got); err != nil || string(got[4:]) != "third" {
		t.Fatalf("reused connection frame = %q, %v", got, err)
	}
	if err := sendContextResult(t, third); err != nil {
		t.Fatal(err)
	}
	if len(n.broadcastSlots) != 0 {
		t.Fatal("broadcast slots leaked")
	}
}

func TestSendContextCancelInterruptsSocketWrite(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "blocked_header"
		if partial {
			name = "partial_frame"
		}
		t.Run(name, func(t *testing.T) {
			n, _, c, receiver := newSendContextPipe(t, 10*time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- n.SendContext(ctx, "peer", []byte("payload")) }()
			sendContextEntered(t, c)
			if partial {
				receiver.SetReadDeadline(time.Now().Add(time.Second))
				prefix := make([]byte, 5)
				if _, err := io.ReadFull(receiver, prefix); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			if err := sendContextResult(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("socket cancellation lost context cause: %v", err)
			}
			if !c.closed.Load() {
				t.Fatal("failed started frame left connection reusable")
			}
			receiver.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := receiver.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("receiver should observe closed incomplete frame: %v", err)
			}
			if len(n.broadcastSlots) != 0 {
				t.Fatal("broadcast slot leaked")
			}
		})
	}
}

func TestWriteFrameContextStarted(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		n, p, _, receiver := newSendContextPipe(t, time.Second)
		read := make(chan error, 1)
		go func() {
			frame := make([]byte, 5)
			_, err := io.ReadFull(receiver, frame)
			if err == nil && !bytes.Equal(frame, []byte{0, 0, 0, 1, 'x'}) {
				err = errors.New("incorrect frame")
			}
			read <- err
		}()
		started, err := n.writeFrameContext(context.Background(), p, []byte{0, 0, 0, 1}, []byte("x"))
		if !started || err != nil {
			t.Fatalf("started=%v err=%v", started, err)
		}
		if err := sendContextResult(t, read); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("canceled_before_lock", func(t *testing.T) {
		n, p, c, _ := newSendContextPipe(t, time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		started, err := n.writeFrameContext(ctx, p, []byte{0, 0, 0, 1}, []byte("x"))
		if started || !errors.Is(err, context.Canceled) {
			t.Fatalf("started=%v err=%v", started, err)
		}
		if c.closed.Load() {
			t.Fatal("untouched connection closed")
		}
	})
	t.Run("socket_failure", func(t *testing.T) {
		n, p, _, receiver := newSendContextPipe(t, time.Second)
		receiver.Close()
		started, err := n.writeFrameContext(context.Background(), p, []byte{0, 0, 0, 1}, []byte("x"))
		if !started || err == nil {
			t.Fatalf("started=%v err=%v", started, err)
		}
	})
}

func TestSendContextTimeoutClosesStartedFrame(t *testing.T) {
	for _, contextFirst := range []bool{false, true} {
		name := "write_timeout"
		if contextFirst {
			name = "context_deadline"
		}
		t.Run(name, func(t *testing.T) {
			writeTimeout, contextTimeout := 100*time.Millisecond, 5*time.Second
			if contextFirst {
				writeTimeout, contextTimeout = contextTimeout, writeTimeout
			}
			n, _, c, _ := newSendContextPipe(t, writeTimeout)
			ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- n.SendContext(ctx, "peer", []byte("x")) }()
			sendContextEntered(t, c)
			err := sendContextResult(t, result)
			if err == nil {
				t.Fatal("blocked send succeeded")
			}
			if contextFirst && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("context deadline cause lost: %v", err)
			}
			if !c.closed.Load() {
				t.Fatal("timed-out started frame left connection reusable")
			}
			if len(n.broadcastSlots) != 0 {
				t.Fatal("timed-out send leaked broadcast slot")
			}
		})
	}
}

func TestWriteFrameContextSingleBudget(t *testing.T) {
	for _, contextFirst := range []bool{false, true} {
		name := "write_timeout_first"
		if contextFirst {
			name = "context_deadline_first"
		}
		t.Run(name, func(t *testing.T) {
			writeTimeout, contextTimeout := 600*time.Millisecond, 3*time.Second
			if contextFirst {
				writeTimeout, contextTimeout = contextTimeout, writeTimeout
			}
			n, p, c, _ := newSendContextPipe(t, writeTimeout)
			p.writeOnce.Do(func() { p.writeSlot = make(chan struct{}, 1) })
			p.writeSlot <- struct{}{}
			ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
			defer cancel()
			begin := time.Now()
			result := make(chan error, 1)
			go func() {
				started, err := n.writeFrameContext(ctx, p, []byte{0, 0, 0, 1}, []byte("x"))
				if !started {
					result <- errors.New("socket write was not marked started")
					return
				}
				result <- err
			}()
			// Spend part of the one budget waiting for the write lock.
			time.Sleep(200 * time.Millisecond)
			<-p.writeSlot
			sendContextEntered(t, c)
			var deadline time.Time
			select {
			case deadline = <-c.deadlines:
			case <-time.After(time.Second):
				t.Fatal("socket write deadline not installed")
			}
			want := begin.Add(writeTimeout)
			if contextFirst {
				want, _ = ctx.Deadline()
			}
			if delta := deadline.Sub(want); delta < -50*time.Millisecond || delta > 100*time.Millisecond {
				t.Errorf("socket deadline = %v, want %v (lock wait must consume budget)", deadline, want)
			}
			err := sendContextResult(t, result)
			if err == nil {
				t.Fatal("blocked socket write succeeded")
			}
			if contextFirst && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("context deadline cause lost: %v", err)
			}
		})
	}
}

func TestWriteFrameContextDeadlineWhileQueued(t *testing.T) {
	n, p, c, _ := newSendContextPipe(t, 5*time.Second)
	p.writeOnce.Do(func() { p.writeSlot = make(chan struct{}, 1) })
	p.writeSlot <- struct{}{}
	defer func() { <-p.writeSlot }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		started, err := n.writeFrameContext(ctx, p, []byte{0, 0, 0, 1}, []byte("x"))
		if started {
			result <- errors.New("queued timeout marked started")
			return
		}
		result <- err
	}()
	if err := sendContextResult(t, result); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued context deadline: %v", err)
	}
	if c.closed.Load() {
		t.Fatal("queued deadline closed connection")
	}
	select {
	case d := <-c.deadlines:
		t.Fatalf("queued deadline changed active socket deadline: %v", d)
	default:
	}
}

// Hold Close open to expose a slot released before the failed socket is closed.
type sendContextCloseGate struct {
	*sendContextConn
	closing      chan struct{}
	release      chan struct{}
	closeOnce    sync.Once
	writes       atomic.Int32
	unsafeWrites atomic.Int32
}

func (c *sendContextCloseGate) Close() error {
	c.closeOnce.Do(func() { close(c.closing) })
	<-c.release
	return c.sendContextConn.Close()
}

func (c *sendContextCloseGate) Write(p []byte) (int, error) {
	select {
	case <-c.closing:
		if !c.closed.Load() {
			c.unsafeWrites.Add(1)
		}
	default:
	}
	c.writes.Add(1)
	return c.sendContextConn.Write(p)
}

func TestSendContextFailedFrameNotReused(t *testing.T) {
	for _, api := range []string{"writeFrameContext", "SendContext"} {
		for _, partial := range []bool{false, true} {
			name := "blocked_header"
			if partial {
				name = "partial_payload"
			}
			t.Run(api+"/"+name, func(t *testing.T) {
				n, p, c, receiver := newSendContextPipe(t, 5*time.Second)
				gate := &sendContextCloseGate{sendContextConn: c, closing: make(chan struct{}), release: make(chan struct{})}
				p.conn = gate
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(gate.release) }) }
				t.Cleanup(release)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				send := func(ctx context.Context, payload string) error {
					if api == "SendContext" {
						return n.SendContext(ctx, "peer", []byte(payload))
					}
					started, err := n.writeFrameContext(ctx, p, []byte{0, 0, 0, byte(len(payload))}, []byte(payload))
					if err == nil && !started {
						return errors.New("successful write not marked started")
					}
					return err
				}
				first := make(chan error, 1)
				go func() { first <- send(ctx, "first") }()
				sendContextEntered(t, c)
				if partial {
					receiver.SetReadDeadline(time.Now().Add(time.Second))
					prefix := make([]byte, 5)
					if _, err := io.ReadFull(receiver, prefix); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(prefix, []byte{0, 0, 0, 5, 'f'}) {
						t.Fatalf("partial frame = %q", prefix)
					}
				}
				second := make(chan error, 1)
				go func() { second <- send(context.Background(), "second") }()
				cancel()
				select {
				case <-gate.closing:
				case <-time.After(2 * time.Second):
					t.Fatal("failed write did not close its connection")
				}
				if len(p.writeSlot) != 1 {
					t.Error("failed writer released slot before closing socket")
				}
				writes := gate.writes.Load()
				wantWrites := int32(1)
				if partial {
					wantWrites = 2 // Header completed, payload is blocked.
				}
				if writes != wantWrites {
					t.Errorf("socket writes before close = %d, want %d", writes, wantWrites)
				}
				release()
				if err := sendContextResult(t, first); !errors.Is(err, context.Canceled) {
					t.Fatalf("first writer cancellation: %v", err)
				}
				if err := sendContextResult(t, second); err == nil {
					t.Fatal("subsequent writer reused failed frame connection")
				}
				if !c.closed.Load() {
					t.Fatal("underlying socket not closed")
				}
				if err := send(context.Background(), "third"); err == nil {
					t.Fatal("later writer reused failed frame connection")
				}
				if count := gate.unsafeWrites.Load(); count != 0 {
					t.Errorf("%d socket writes reused failed connection before close completed", count)
				}
				receiver.SetReadDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 32)
				if count, err := receiver.Read(buf); count != 0 || !errors.Is(err, io.EOF) {
					t.Fatalf("bytes after failed frame = %q, error = %v; want EOF", buf[:count], err)
				}
				if len(n.broadcastSlots) != 0 || len(p.writeSlot) != 0 {
					t.Fatal("failed writers leaked slots")
				}
			})
		}
	}
}
