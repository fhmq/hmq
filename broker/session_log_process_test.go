package broker

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

// This exercises process-crash recovery, not power-loss durability.
func TestSessionLogProcessSIGKILLRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires SIGKILL")
	}
	if path := os.Getenv("HMQ_SESSION_LOG_PROCESS_TEST"); path != "" {
		b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		fmt.Println("SESSION_PROCESS READY " + listener.Addr().String())
		for {
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				defer conn.Close()
				_ = b.handleConnection(CLIENT, conn)
				fmt.Println("SESSION_PROCESS CLOSED")
			}()
		}
	}

	path := filepath.Join(t.TempDir(), "sessions.log")
	start := func() (string, func(), <-chan string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSessionLogProcessSIGKILLRecovery$", "-test.timeout=40s")
		cmd.Env = append(os.Environ(), "HMQ_SESSION_LOG_PROCESS_TEST="+path)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		lines := make(chan string, 16)
		go func() {
			defer close(lines)
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				if line := scanner.Text(); strings.HasPrefix(line, "SESSION_PROCESS ") {
					select {
					case lines <- line:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		stopped := false
		kill := func() {
			t.Helper()
			if stopped {
				return
			}
			stopped = true
			defer cancel()
			if err := cmd.Process.Kill(); err != nil {
				t.Errorf("SIGKILL child: %v", err)
			}
			select {
			case err := <-done:
				if _, ok := err.(*exec.ExitError); !ok {
					t.Errorf("expected killed child, got %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("timeout reaping child")
			}
		}
		t.Cleanup(kill)
		select {
		case line := <-lines:
			if strings.HasPrefix(line, "SESSION_PROCESS READY ") {
				return strings.TrimPrefix(line, "SESSION_PROCESS READY "), kill, lines
			}
			t.Fatalf("child readiness: %q", line)
		case <-time.After(10 * time.Second):
			t.Fatal("child readiness timeout")
		}
		return "", kill, lines
	}
	connect := func(addr, id string, present bool) net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		sendTestConnect(t, c, id, false)
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		if ack, ok := p.(*packets.ConnackPacket); !ok || ack.ReturnCode != 0 || ack.SessionPresent != present {
			t.Fatalf("CONNACK: %#v", p)
		}
		return c
	}
	write := func(c net.Conn, p packets.ControlPacket) {
		t.Helper()
		if err := p.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	read := func(c net.Conn) packets.ControlPacket {
		t.Helper()
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	ping := func(c net.Conn) {
		t.Helper()
		write(c, packets.NewControlPacket(packets.Pingreq))
		p := read(c)
		if _, ok := p.(*packets.PingrespPacket); !ok {
			t.Fatalf("expected PINGRESP (no pending delivery), got %#v", p)
		}
	}
	publish := func(c net.Conn, payload string, id uint16) {
		t.Helper()
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName, p.Payload, p.Qos, p.MessageID = "process/a", []byte(payload), 1, id
		write(c, p)
		if ack, ok := read(c).(*packets.PubackPacket); !ok || ack.MessageID != id {
			t.Fatalf("publisher PUBACK: %#v", ack)
		}
	}
	delivery := func(c net.Conn, payload string) {
		t.Helper()
		p := read(c)
		msg, ok := p.(*packets.PublishPacket)
		if !ok || msg.TopicName != "process/a" || string(msg.Payload) != payload || msg.Qos != 1 || msg.MessageID == 0 || msg.Retain {
			t.Fatalf("delivery: %#v", p)
		}
		ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		ack.MessageID = msg.MessageID
		write(c, ack)
		// Same-connection PINGRESP is a barrier after processing PUBACK.
		ping(c)
	}

	addr, kill, lines := start()
	sub := connect(addr, "process-durable", false)
	request := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	request.MessageID, request.Topics, request.Qoss = 1, []string{"process/+"}, []byte{1}
	write(sub, request)
	if ack, ok := read(sub).(*packets.SubackPacket); !ok || ack.MessageID != 1 || len(ack.ReturnCodes) != 1 || ack.ReturnCodes[0] != 1 {
		t.Fatalf("SUBACK: %#v", ack)
	}
	write(sub, packets.NewControlPacket(packets.Disconnect))
	sub.Close()
	select {
	case line := <-lines:
		if line != "SESSION_PROCESS CLOSED" {
			t.Fatalf("offline barrier: %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber did not go offline")
	}
	pub := connect(addr, "process-publisher", false)
	publish(pub, "confirmed-before-kill", 9)
	kill() // No Broker.Close or graceful process shutdown after publisher PUBACK.
	pub.Close()

	addr, kill, _ = start()
	sub = connect(addr, "process-durable", true)
	delivery(sub, "confirmed-before-kill")
	kill() // ACK processing was confirmed by PINGRESP before this crash.
	sub.Close()

	addr, _, _ = start()
	sub = connect(addr, "process-durable", true)
	ping(sub)
	if err := sub.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if p, err := packets.ReadPacket(sub); err == nil {
		t.Fatalf("unexpected replay after durable ACK: %#v", p)
	} else if e, ok := err.(net.Error); !ok || !e.Timeout() {
		t.Fatalf("checking absence of replay: %v", err)
	}
	if err := sub.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// No SUBSCRIBE after either restart: new traffic proves filter recovery.
	pub = connect(addr, "process-publisher", true)
	publish(pub, "new-after-recovery", 10)
	delivery(sub, "new-after-recovery")
}
