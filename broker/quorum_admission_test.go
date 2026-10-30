package broker

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/replication"
	"github.com/fhmq/hmq/v3/broker/storage"
)

func TestQuorumAdmissionConcurrentMQTTPublishers(t *testing.T) {
	const publishers, perPublisher = 4, 6
	const total = publishers * perPublisher
	configs := quorumTestConfigs(t)
	var nodes []*Broker
	for i := range configs {
		configs[i].Host, configs[i].Port = "127.0.0.1", "0"
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		nodes = append(nodes, b)
		b.Start()
	}
	leader := nodes[quorumLeader(t, nodes)]
	connections := make([]net.Conn, publishers)
	for i := range connections {
		connections[i] = quorumConnect(t, leader, fmt.Sprintf("admission-publisher-%d", i))
	}
	start := make(chan struct{})
	results := make(chan error, publishers)
	for id, conn := range connections {
		go func(id int, conn net.Conn) {
			<-start
			results <- func() error {
				for sequence := 0; sequence < perPublisher; sequence++ {
					if err := conn.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
						return err
					}
					payload := fmt.Sprintf("publisher-%d/message-%d", id, sequence)
					p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
					p.TopicName, p.Qos, p.MessageID = "quorum/admission", 1, uint16(sequence+1)
					p.Payload = []byte(payload)
					if err := p.Write(conn); err != nil {
						return fmt.Errorf("%s write: %w", payload, err)
					}
					packet, err := packets.ReadPacket(conn)
					if err != nil {
						return fmt.Errorf("%s ACK: %w", payload, err)
					}
					ack, ok := packet.(*packets.PubackPacket)
					if !ok || ack.MessageID != p.MessageID {
						return fmt.Errorf("%s unexpected ACK: %#v", payload, packet)
					}
					entries, err := leader.quorumRuntime.node.Entries(0, total+1)
					if err != nil {
						return err
					}
					found := false
					for _, entry := range entries {
						var msg clusterPublish
						if err := stdjson.Unmarshal(entry.Payload, &msg); err != nil {
							return err
						}
						if msg.Topic == p.TopicName && string(msg.Payload) == payload && msg.QoS == 1 && !msg.Retain {
							found = true
							break
						}
					}
					if !found {
						return fmt.Errorf("%s acknowledged before leader commit", payload)
					}
				}
				return nil
			}()
		}(id, conn)
	}
	close(start)
	// Collect every worker before reporting failures or closing its broker.
	for i := 0; i < publishers; i++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		return
	}
	want, err := leader.quorumRuntime.node.Entries(0, total+1)
	if err != nil || len(want) != total {
		t.Fatalf("leader committed count = %d, want %d: %v", len(want), total, err)
	}
	for id := 0; id < publishers; id++ {
		next := 0
		for _, entry := range want {
			var msg clusterPublish
			if err := stdjson.Unmarshal(entry.Payload, &msg); err != nil {
				t.Fatal(err)
			}
			var publisher, sequence int
			if _, err := fmt.Sscanf(string(msg.Payload), "publisher-%d/message-%d", &publisher, &sequence); err != nil {
				t.Fatal(err)
			}
			if publisher == id {
				if sequence != next {
					t.Fatalf("publisher %d sequence = %d, want %d", id, sequence, next)
				}
				next++
			}
		}
		if next != perPublisher {
			t.Fatalf("publisher %d committed %d, want %d", id, next, perPublisher)
		}
	}
	for i, b := range nodes {
		deadline := time.Now().Add(8 * time.Second)
		for {
			got, err := b.quorumRuntime.node.Entries(0, total+1)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(got, want) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replica %d did not converge: got %d entries, want %d with identical order/content", i, len(got), total)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func quorumAdmissionWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("proposal operation blocked")
	}
}

func TestQuorumAdmissionBoundedConcurrency(t *testing.T) {
	if quorumProposalLimit != 128 {
		t.Fatalf("proposal limit = %d, want 128", quorumProposalLimit)
	}
	r := &quorumRuntime{proposals: make(chan struct{}, 128)}
	const attempts = 4 * 128
	for round := 0; round < 2; round++ {
		start := make(chan struct{})
		results := make(chan error, attempts)
		for i := 0; i < attempts; i++ {
			go func() {
				<-start
				results <- r.acquireProposal()
			}()
		}
		close(start)
		done := make(chan struct{})
		go func() {
			accepted, busy := 0, 0
			for i := 0; i < attempts; i++ {
				switch err := <-results; {
				case err == nil:
					accepted++
				case errors.Is(err, replication.ErrBusy):
					busy++
				default:
					t.Errorf("admission: %v", err)
				}
			}
			if accepted != 128 || busy != attempts-128 {
				t.Errorf("accepted=%d busy=%d, want 128/%d", accepted, busy, attempts-128)
			}
			close(done)
		}()
		// No proposal is released until every overload request has returned.
		quorumAdmissionWait(t, done)
		if got := len(r.proposals); got != 128 {
			t.Fatalf("occupied slots = %d, want 128", got)
		}
		r.releaseProposal()
		if err := r.acquireProposal(); err != nil {
			t.Fatalf("admission after releasing one slot: %v", err)
		}
		var releases sync.WaitGroup
		for i := 0; i < 128; i++ {
			releases.Add(1)
			go func() { defer releases.Done(); r.releaseProposal() }()
		}
		drained := make(chan struct{})
		go func() { releases.Wait(); r.proposalWG.Wait(); close(drained) }()
		quorumAdmissionWait(t, drained)
		if got := len(r.proposals); got != 0 {
			t.Fatalf("slots after drain = %d", got)
		}
	}
}

func TestQuorumAdmissionRejectsUnhealthyAndClosed(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "full"}[full], func(t *testing.T) {
			r := &quorumRuntime{proposals: make(chan struct{}, 128)}
			if full {
				for i := 0; i < 128; i++ {
					if err := r.acquireProposal(); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := len(r.proposals)
			r.mu.Lock()
			r.consumeErr = errors.New("consumer failed")
			r.mu.Unlock()
			if err := r.acquireProposal(); err == nil || errors.Is(err, replication.ErrBusy) {
				t.Fatalf("unhealthy admission = %v, want consumer error", err)
			}
			r.mu.Lock()
			r.closed = true
			r.mu.Unlock()
			if err := r.acquireProposal(); !errors.Is(err, storage.ErrClosed) {
				t.Fatalf("closed admission = %v, want ErrClosed", err)
			}
			if len(r.proposals) != before {
				t.Fatal("rejected admission changed occupied slots")
			}
			for i := 0; i < before; i++ {
				r.releaseProposal()
			}
			done := make(chan struct{})
			go func() { r.proposalWG.Wait(); close(done) }()
			quorumAdmissionWait(t, done)
		})
	}
}

func TestQuorumAdmissionClosedWait(t *testing.T) {
	for _, outstanding := range []int{0, 1, 128} {
		r := &quorumRuntime{proposals: make(chan struct{}, 128)}
		for i := 0; i < outstanding; i++ {
			if err := r.acquireProposal(); err != nil {
				t.Fatal(err)
			}
		}
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		start := make(chan struct{})
		waiting := make(chan struct{})
		waited := make(chan struct{})
		go func() {
			<-start
			close(waiting)
			r.proposalWG.Wait()
			close(waited)
		}()
		var callers sync.WaitGroup
		for i := 0; i < 256; i++ {
			callers.Add(1)
			go func() {
				defer callers.Done()
				<-start
				if err := r.acquireProposal(); !errors.Is(err, storage.ErrClosed) {
					t.Errorf("admission after close = %v, want ErrClosed", err)
					if err == nil {
						r.releaseProposal()
					}
				}
			}()
		}
		close(start)
		quorumAdmissionWait(t, waiting)
		if outstanding > 0 {
			select {
			case <-waited:
				t.Fatal("Wait returned with outstanding proposals")
			case <-time.After(20 * time.Millisecond):
			}
		}
		for i := 0; i < outstanding; i++ {
			r.releaseProposal()
		}
		quorumAdmissionWait(t, waited)
		done := make(chan struct{})
		go func() { callers.Wait(); close(done) }()
		quorumAdmissionWait(t, done)
		if len(r.proposals) != 0 {
			t.Fatal("closed runtime admitted new proposals")
		}
	}
}
