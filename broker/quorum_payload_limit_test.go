package broker

import (
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/replication"
)

func TestQuorumRejectsOversizedPayloadBeforeAdmission(t *testing.T) {
	p := &packets.PublishPacket{TopicName: "test", Payload: make([]byte, replication.MaxPayloadSize+1)}
	if validQuorumPublication(p) {
		t.Fatal("oversized payload accepted")
	}
	// No Raft node or admission capacity is needed to reject an oversized call.
	r := &quorumRuntime{}
	if err := r.publish(p); err != ErrQuorumRoutingUnavailable {
		t.Fatalf("unexpected rejection: %v", err)
	}
}
